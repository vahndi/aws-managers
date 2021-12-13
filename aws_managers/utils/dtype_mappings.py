from sagemaker.feature_store.feature_definition import FeatureTypeEnum

# FeatureStore Type Names to ENUMS
FS_NAME_TO_FS_ENUM = {
    'String': FeatureTypeEnum.STRING,
    'Integral': FeatureTypeEnum.INTEGRAL,
    'Fractional': FeatureTypeEnum.FRACTIONAL
}

# FeatureStore Type Names to Athena Type Names
FS_NAME_TO_ATHENA_NAME = {
    'String': 'string',
    'Integral': 'int',
    'Fractional': 'double'
}
