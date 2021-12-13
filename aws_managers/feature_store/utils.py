from time import strftime, gmtime, time, sleep
from typing import Optional, Dict

from pandas import DataFrame, Series
from sagemaker import Session, get_execution_role
from sagemaker.feature_store.feature_definition import \
    FeatureDefinition, FeatureTypeEnum
from sagemaker.feature_store.feature_group import FeatureGroup

from aws_managers.utils.dtype_mappings import FS_NAME_TO_FS_ENUM


def make_feature_group(
        group_name: str,
        description: str,
        data: DataFrame,
        s3_uri: str,
        identifier_name: str,
        identifier_type: str,
        append_time_to_name: bool = True,
        feature_types: Optional[Dict[str, str]] = None,
        enable_online_store: bool = False,
        disable_glue_table_creation: bool = True,
        max_workers: int = 4
) -> FeatureGroup:
    """
    Make a new FeatureGroup.

    :param group_name: Name of the FeatureGroup.
    :param description: Description of the FeatureGroup.
    :param data: The data to ingest into the FeatureGroup.
    :param s3_uri: The S3 URI to create the FeatureGroup in.
    :param identifier_name: Name of the column to use as an index.
    :param identifier_type: Type of the identifier column. N.B. to create a
                            string column use series.astype('string')
    :param append_time_to_name: Whether to append the current datetime to the
                                name of the created FeatureGroup.
    :param feature_types: Mapping of feature names to types. Types should be one
                          of {'String', 'Integral', 'Fractional'}.
    :param enable_online_store: Whether to create an online store or not.
    :param disable_glue_table_creation: Whether to not create an AWS Glue table.
    """
    # create feature definitions
    feature_definitions = [
        FeatureDefinition(feature_name=identifier_name,
                          feature_type=FS_NAME_TO_FS_ENUM[identifier_type]),
        FeatureDefinition(feature_name='EventTime',
                          feature_type=FeatureTypeEnum.FRACTIONAL)
    ]
    if feature_types is not None:
        for name, data_type in feature_types.items():
            feature_definitions.append(FeatureDefinition(
                feature_name=name,
                feature_type=FS_NAME_TO_FS_ENUM[data_type]
            ))
    # define feature group
    session = Session()
    if append_time_to_name:
        group_name += '---' + strftime('%Y-%m-%d--%H-%M', gmtime())
    feature_group = FeatureGroup(
        name=group_name,
        sagemaker_session=session,
        feature_definitions=feature_definitions
    )
    # create feature group
    role = get_execution_role()
    current_time_sec = int(round(time()))
    data['EventTime'] = Series(data=[current_time_sec] * len(data),
                               dtype='float64')
    feature_group.create(
        s3_uri=s3_uri,
        record_identifier_name=identifier_name,
        event_time_feature_name='EventTime',
        role_arn=role,
        description=description,
        enable_online_store=enable_online_store,
        disable_glue_table_creation=disable_glue_table_creation
    )
    # wait for successful creation
    status = feature_group.describe().get("FeatureGroupStatus")
    num_tries = 0
    while status == "Creating":
        print("\rWaiting for Feature Group to be Created " + '.' * num_tries,
              end='')
        sleep(1)
        num_tries += 1
        status = feature_group.describe().get("FeatureGroupStatus")
    print()
    if status == 'Created':
        print(f"FeatureGroup {feature_group.name} successfully created.")
    else:
        print(feature_group.describe().get("FailureReason"))
        raise ValueError(f"FeatureGroup creation failed (status='{status}')")
    # ingest data
    print('Ingesting data - this could take a while...')
    t_start = time()
    feature_group.ingest(
        data_frame=data,
        max_workers=max_workers,
        wait=True
    )
    t_end = time()
    print(f'Done in {t_end - t_start}s.')
    return feature_group