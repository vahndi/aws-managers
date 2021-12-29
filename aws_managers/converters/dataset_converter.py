from numpy import nan
from pandas import DataFrame, Series, Categorical, notnull
from time import time
from tqdm import tqdm


class DatasetConverter(object):

    def __init__(
            self,
            columns: DataFrame,
            categories: DataFrame,
            old_name: str,
            new_name: str,
            d_type: str,
            nulls: str,
            category: str,
            old_value: str,
            new_value: str
    ):
        """
        Create a new DatasetConverter.

        Contains 3 main methods and several sub-methods for validating and
        converting a dataset.

        Main methods are:
            * validate_metadata
            * validate_data
            * convert_data

        :param columns: DataFrame of column metadata. Each row represents a
                        column of the dataset.
        :param categories: DataFrame of category metadata. Each row represents a
                           unique value of a category column in the dataset.
        :param old_name: Name of column in `columns` containing names of columns
                         in the original dataset.
        :param new_name: Name of column in `columns` containing names of columns
                         in the new dataset.
        :param d_type: Name of the column in `columns` containing the d-type of
                       columns in the new dataset.
        :param nulls: Name of the column in `columns` specifying whether the
                      respective column is allowed to contain null values.
        :param category: Name of the column in `columns` and in `categories`
                         containing category names for linking the 2 sets.
        :param old_value: Name of the column in `categories` containing the
                          old values of each category in the dataset.
        :param new_value: Name of the column in `categories` containing the
                          new values of each category in the dataset.
        """
        self.columns: DataFrame = columns
        self.categories: DataFrame = categories
        self.old_name: str = old_name
        self.new_name: str = new_name
        self.d_type: str = d_type
        self.nulls: str = nulls
        self.category: str = category
        self.old_value: str = old_value
        self.new_value: str = new_value
        self.n_cols = len(self.columns)

    def check_old_names_unique(self):
        """
        Check that there are no repeated values in the `old_name` column of the
        columns metadata.
        """
        if self.columns[self.old_name].nunique() != self.n_cols:
            counts = self.columns[self.old_name].value_counts()
            repeated = counts.loc[counts > 1]
            raise ValueError(
                f"columns['{self.old_name}'] has repeats:\n{repeated}"
            )

    def check_new_names_unique(self):
        """
        Check that there are no repeated values in the `new_name` column of the
        columns metadata.
        """
        if self.columns[self.new_name].nunique() != self.n_cols:
            counts = self.columns[self.new_name].value_counts()
            repeated = counts.loc[counts > 1]
            raise ValueError(
                f"columns['{self.new_name}'] has repeats:\n{repeated}"
            )

    def check_old_name_spaces(self):
        """
        Check that there are no leading or trailing spaces in the values of
        `old_name`.
        """
        cols_with_spaces = []
        for old_name in self.columns[self.old_name].unique():
            if old_name.startswith(' ') or old_name.endswith(' '):
                cols_with_spaces.append(old_name)
        if len(cols_with_spaces):
            raise ValueError(
                f"{len(cols_with_spaces)} values in columns['{self.old_name}']"
                f'have spaces at the start or end:\n'
                f'{cols_with_spaces}'
            )

    def check_new_name_spaces(self):
        """
        Check that there are no leading or trailing spaces in the values of
        `new_name`.
        """
        cols_with_spaces = []
        for new_name in self.columns[self.new_name].unique():
            if new_name.startswith(' ') or new_name.endswith(' '):
                cols_with_spaces.append(new_name)
        if len(cols_with_spaces):
            raise ValueError(
                f"{len(cols_with_spaces)} values in columns['{self.new_name}']"
                f'have spaces at the start or end:\n'
                f'{cols_with_spaces}'
            )

    def check_categories_have_values(self):
        """
        Check that each category column in the columns metadata has associated
        values in the categories metadata.
        """
        expected_categories = set(
            self.columns.loc[
                self.columns[self.d_type] == 'category',
                self.category
            ].to_list()
        )
        actual_categories = set(self.categories[self.category].unique())
        if not expected_categories.issubset(actual_categories):
            difference = sorted(
                expected_categories.difference(actual_categories)
            )
            raise ValueError(
                f'The following category columns do not have values:\n'
                f'{difference}'
            )

    def check_old_values_unique(self):
        """
        Check that the 'old_value's for each category in the categories metadata
        are unique.
        """
        repeats = []
        for category_name in self.categories[self.category].unique():
            category_value_counts = self.categories.loc[
                self.categories[self.category] == category_name,
                self.old_value
            ].value_counts(dropna=False)
            if category_value_counts.max() > 1:
                repeats.append({
                    'category': category_name,
                    'repeats': category_value_counts.loc[
                        category_value_counts > 1
                    ].to_dict()
                })
        if len(repeats):
            raise ValueError(
                f'Found {len(repeats)} categories with repeated old_value:\n'
                f'{repeats}'
            )

    def check_new_values_unique(self):
        """
        Check that the 'new_value's for each category in the categories metadata
        are unique.
        """
        repeats = []
        for category_name in self.categories[self.category].unique():
            category_value_rows = self.categories.loc[
                self.categories[self.category] == category_name
            ]
            if category_value_rows[self.new_value].notnull().sum() == 0:
                # no new values
                continue
            category_value_counts = category_value_rows[
                self.new_value
            ].value_counts(dropna=False)
            if category_value_counts.max() > 1:
                repeats.append({
                    'category': category_name,
                    'repeats': category_value_counts.loc[
                        category_value_counts > 1
                    ].to_dict()
                })
        if len(repeats):
            raise ValueError(
                f'Found {len(repeats)} categories with repeated new_value:\n'
                f'{repeats}'
            )

    def validate_metadata(self):
        """
        Run all metadata checks.
        """
        print('validating metadata...')
        self.check_old_names_unique()
        self.check_new_names_unique()
        self.check_old_name_spaces()
        self.check_new_name_spaces()
        self.check_categories_have_values()
        self.check_old_values_unique()
        self.check_new_values_unique()

    def check_old_names(self, data: DataFrame):
        """
        Check that all column names in the `columns` metadata exist in the data.
        """
        expected_names = set(self.columns[self.old_name])
        actual_names = set(data.columns)
        if not expected_names.issubset(actual_names):
            difference = expected_names.difference(actual_names)
            raise ValueError(
                f'dataset is missing columns expected in metadata:\n'
                f'{difference}'
            )

    def check_categories(self, data: DataFrame):
        """
        Check that category values in the data are a subset of those in the
        metadata.
        """
        sentinel = f'@#${time()}$#@'  # used because nan != nan
        mismatches = []
        cat_columns = self.columns.loc[self.columns[self.d_type] == 'category']
        for _, column in cat_columns.iterrows():
            category = column[self.category]
            old_name = column[self.old_name]
            category_value_rows = self.categories.loc[
                self.categories[self.category] == category
            ]
            # find expected and actual values
            expected_values = set(
                category_value_rows[self.old_value].fillna(sentinel).unique()
            )
            if column[self.nulls] is True and sentinel not in expected_values:
                expected_values.add(sentinel)
            actual_values = set(
                data.loc[:, old_name].fillna(sentinel).unique()
            )
            # compare
            if not actual_values.issubset(expected_values):
                mismatches.append({
                    'column': old_name,
                    'category': category,
                    'missing_in_metadata':
                        actual_values.difference(expected_values)
                })
        if len(mismatches):
            raise ValueError(
                f'{len(mismatches)} categories have values present in data '
                f'but not in definitions:\n'
                f'{mismatches}'
            )

    def check_nulls(self, data: DataFrame):
        """
        Check that columns marked with null=False do not contain any null
        values.
        """
        null_counts = []
        for _, column in tqdm(self.columns.iterrows(), total=self.n_cols):
            if column[self.nulls] == False:
                num_nulls = data[column[self.old_name]].isnull().sum()
                if num_nulls > 0:
                    null_counts.append({
                        'column': column[self.old_name],
                        'nulls': num_nulls
                    })
        if len(null_counts) > 0:
            raise ValueError(
                f'{len(null_counts)} columns marked as '
                f'not having nulls have them:\n'
                f'{null_counts}'
            )

    def validate_data(self, data: DataFrame):
        """
        Run all data checks.
        """
        print('validating data...')
        self.check_old_names(data)
        self.check_categories(data)
        self.check_nulls(data)

    def convert_categorical_values(self, data: DataFrame) -> DataFrame:
        """
        Convert values of categorical columns from `old_value` to `new_value`.
        """
        cat_columns = self.columns.loc[self.columns[self.d_type] == 'category']
        sentinel = f'@#${time()}$#@'  # used because nan != nan
        for _, column in tqdm(cat_columns.iterrows(), total=len(cat_columns)):
            category_name = column[self.category]
            category_value_rows = self.categories.loc[
                self.categories[self.category] == category_name
            ]
            if category_value_rows[self.new_value].notnull().sum() == 0:
                # no new values
                continue
            # replace nan with sentinel in old values
            old_name: str = column[self.old_name]
            data[old_name] = data[old_name].fillna(sentinel)
            new_values = category_value_rows.copy()
            new_values[self.old_value] = new_values[
                self.old_value
            ].fillna(sentinel)
            # populate new values
            new_values = new_values.set_index(self.old_value)[
                self.new_value
            ].to_dict()
            data[old_name] = data[old_name].replace(new_values)
            data[old_name] = data[old_name].replace(sentinel, nan)
        return data

    def convert_d_types(self, data: DataFrame):
        """
        Convert data types of the columns in data.
        """
        for _, column in tqdm(self.columns.iterrows(), total=self.n_cols):
            old_name: str = column[self.old_name]
            category: str = column[self.category]
            d_type: str = column[self.d_type]
            if d_type == 'int':
                if column[self.nulls] == True:
                    data[old_name] = data[old_name].astype(float)
                else:
                    data[old_name] = data[old_name].astype(int)
            elif d_type == 'category':
                category_value_rows = self.categories.loc[
                    self.categories[self.category] == category
                ]
                if category_value_rows[self.new_value].notnull().sum() > 0:
                    category_values = category_value_rows[
                        self.new_value
                    ].dropna().to_list()
                else:
                    category_values = category_value_rows[
                        self.old_value
                    ].dropna().to_list()
                data[old_name] = Series(
                    data=Categorical(values=data[old_name].values,
                                     categories=category_values),
                    index=data.index
                )
                data[old_name] = data[old_name].cat.reorder_categories(
                    new_categories=category_values,
                    ordered=True
                )
            else:
                if notnull(d_type):
                    try:
                        data[old_name] = data[old_name].astype(d_type)
                    except TypeError:
                        print(
                            f"Warning: can't convert {old_name} "
                            f"to type {d_type}"
                        )
        return data

    def rename_columns(self, data: DataFrame) -> DataFrame:
        """
        Rename the columns of the data from `old_name`s to `new_name`s.
        """
        data = data.rename(
            columns=self.columns.set_index(self.old_name)[
                self.new_name
            ].to_dict()
        )
        return data

    def convert_data(self, data: DataFrame, copy: bool = False) -> DataFrame:
        """
        Convert the dataset.

        :param data: Dataset to convert.
        :param copy: Whether to make a copy of the dataset before conversion so
                     that any uncaught errors will not affect the dataset.
        """
        print('converting data...')
        if copy is True:
            data = data.copy()
        data = self.convert_categorical_values(data)
        data = self.convert_d_types(data)
        data = self.rename_columns(data)
        return data
