import pandas as pd
import re

from .replacements import FIELD_NAMES


def drop_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove columns that contain only NA values.

    Args:
        df (pd.DataFrame): DataFrame to be processed.

    Returns:
        pd.DataFrame: DataFrame with empty columns removed.
    """
    return df.dropna(axis='columns', how='all')


def merge_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge duplicate columns in a DataFrame by taking the first non-NA value.

    Args:
        df (pd.DataFrame): DataFrame to be processed.

    Returns:
        pd.DataFrame: DataFrame with duplicate columns merged.
    """
    return (df
            .T
            .groupby(df.columns, sort=False)
            .apply(lambda x: x.infer_objects(copy=False).bfill().iloc[0])
            .T
            )


def replace_column_name(col):
    """
    Replace substrings in a (single) column name based on FIELD_NAMES dictionary.
    Args:
        col (str): Column name to be processed.
    Returns:
        str: Column name with substrings replaced.
    """
    for old, new in FIELD_NAMES.items():
        col = re.sub(old, new, col)
    return col


def replace_strings(series: pd.Series, replacements: dict) -> pd.Series:
    """
    Replace substrings in a pandas Series based on a replacements dictionary.
    Args:
        series (pd.Series): Series to be processed.
        replacements (dict): Dictionary with substrings to be replaced as keys and their replacements as values.
    Returns:
        pd.Series: Series with substrings replaced.
    """
    for old, new in replacements.items():
        series = series.str.replace(old, new, regex=True)
    return series


def fill_participant_ids(df):
    """
    Fill missing participant IDs in the DataFrame and format them as ABD001.
    To be used with EMA data.

    Args:
        df (pd.DataFrame): DataFrame to be processed.
    Returns:
        pd.DataFrame: DataFrame with participant IDs filled and formatted.
    """
    # We do not want to fill IDs across EMA periods, so we do it by EMA period
    def fill_and_format(s):
        return (
            s.infer_objects(copy=False)
             .ffill()
             .astype(int)
             .apply(lambda x: f"ABD{x:03d}")
        )
    df['participant_id'] = (
        df.groupby('EMA_period_number')['participant_id']
          .transform(fill_and_format)
    )
    return df


def get_ema_period_number(project_title: str) -> int:
    """
    Extract the EMA period number from the project title.
    The function will extract the number following "PERIOD " in the title.

    Args:
        project_title (str): The title of the REDCap project.
    Returns:
        int: The EMA period number.
    Raises:
        ValueError: If the period number cannot be extracted from the title string.
    """
    match = re.search(r'PERIOD (\d+)', project_title)
    if match:
        return int(match.group(1))
    else:
        raise ValueError('Could not extract EMA period number from project title.')


def fix_24h_sleeptimes(df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    In EMA data, sleeptimes are sometimes incorrectly recorded in 12-hour format by participants.
    This function detects sleeptimes that are between 6am and 12pm, and shifts them by 12 hours.

    Args:
        df (pd.DataFrame): DataFrame to be processed.
        logger: Logger instance for logging warnings.
    Returns:
        pd.DataFrame: DataFrame with corrected sleeptimes.
    """
    flags = df.query('try_sleep_time >= "06:00:00" and try_sleep_time < "12:00:00"')
    flagged_participants = sorted(flags['participant_id'].unique())
    if len(flagged_participants) > 0:
        logger.warning(f'Correcting sleeptimes for participants: {flagged_participants}')
        df.loc[flags.index, 'try_sleep_time'] = (
            pd.to_datetime(
                df.loc[flags.index, 'try_sleep_time'],
                format='%H:%M:%S'
            ) + pd.Timedelta(hours=12)
        ).dt.time
    return df
