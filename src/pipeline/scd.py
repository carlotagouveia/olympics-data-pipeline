from __future__ import annotations

from datetime import date

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)

_FAR_FUTURE: pd.Timestamp = pd.Timestamp("9999-12-31")


def apply_scd_type1(
    existing: pd.DataFrame,
    incoming: pd.DataFrame,
    natural_key: str,
    tracked_cols: list[str],
) -> pd.DataFrame:
    """Apply SCD Type 1: overwrite changed attributes, append new records.

    Parameters
    ----------
    existing:
        Current state of the dimension table (may be empty on first load).
    incoming:
        New batch of dimension records.
    natural_key:
        Column name that uniquely identifies a business entity (e.g. ``event_name``).
    tracked_cols:
        Columns whose values should be overwritten when they change.

    Returns
    -------
    pd.DataFrame
        Updated dimension table.
    """
    if existing.empty:
        logger.debug("SCD1 – first load, returning incoming as-is")
        return incoming.copy()

    existing_nks: set[object] = set(existing[natural_key])
    incoming_nks: set[object] = set(incoming[natural_key])

    # update existing records
    updated = existing.copy()
    changed_nks = incoming_nks & existing_nks
    if changed_nks:
        updates = incoming[incoming[natural_key].isin(changed_nks)].set_index(natural_key)
        for col in tracked_cols:
            if col in updates.columns:
                updated = updated.set_index(natural_key)
                updated.loc[updated.index.isin(changed_nks), col] = updates[col]
                updated = updated.reset_index()
        logger.debug("SCD1 – overwrote %s records", len(changed_nks))

    #  append brand-new records 
    new_nks = incoming_nks - existing_nks
    if new_nks:
        new_records = incoming[incoming[natural_key].isin(new_nks)]
        updated = pd.concat([updated, new_records], ignore_index=True)
        logger.debug("SCD1 – appended %s new records", len(new_nks))

    return updated


def apply_scd_type2(
    existing: pd.DataFrame,
    incoming: pd.DataFrame,
    natural_key: str,
    tracked_cols: list[str],
    surrogate_key: str,
    load_date: date,
) -> pd.DataFrame:
    """Apply SCD Type 2: expire changed records and insert new versions.

    Parameters
    ----------
    existing:
        Current dimension table with SCD metadata columns:
        ``surrogate_key``, ``valid_from``, ``valid_to``, ``is_current``.
        May be empty on first load.
    incoming:
        New batch of dimension records (no SCD metadata columns required).
    natural_key:
        Business key column (e.g. ``athlete_nk``).
    tracked_cols:
        Columns that trigger a new SCD version when their value changes.
    surrogate_key:
        Name of the auto-generated integer surrogate key column.
    load_date:
        The effective date of this batch (used as ``valid_from`` for new rows
        and as ``valid_to`` for expired rows).

    Returns
    -------
    pd.DataFrame
        Full dimension table with historical and current records.
    """
    load_ts: pd.Timestamp = pd.Timestamp(load_date)

    # first load: assign surrogate keys and SCD metadata, return directly
    if existing.empty:
        result = incoming.copy()
        result[surrogate_key] = range(1, len(result) + 1)
        result["valid_from"] = load_ts
        result["valid_to"] = _FAR_FUTURE
        result["is_current"] = True
        logger.debug("SCD2 – first load, inserted %s records", len(result))
        return result

    max_sk: int = int(existing[surrogate_key].max())
    next_sk: int = max_sk + 1

    current_records: pd.DataFrame = existing[existing["is_current"]].copy()

    # detect changed records
    comparison: pd.DataFrame = current_records.merge(
        incoming[[natural_key] + tracked_cols],
        on=natural_key,
        how="inner",
        suffixes=("_curr", "_new"),
    )

    changed_mask: pd.Series = pd.Series(False, index=comparison.index)  # type: ignore[type-arg]
    for col in tracked_cols:
        old_col = f"{col}_curr"
        new_col = f"{col}_new"
        if old_col in comparison.columns and new_col in comparison.columns:
            old_vals: pd.Series = comparison[old_col].fillna("").astype(str)  # type: ignore[type-arg]
            new_vals: pd.Series = comparison[new_col].fillna("").astype(str)  # type: ignore[type-arg]
            changed_mask = changed_mask | (old_vals != new_vals)

    changed_nks: list[object] = comparison.loc[changed_mask, natural_key].tolist()
    logger.debug("SCD2 – detected %s changed records", len(changed_nks))

    # expire changed current records
    result = existing.copy()
    expire_mask: pd.Series = result[natural_key].isin(changed_nks) & result["is_current"]  # type: ignore[type-arg]
    result.loc[expire_mask, "valid_to"] = load_ts
    result.loc[expire_mask, "is_current"] = False

    
    # insert new versions for changed records
    new_versions: pd.DataFrame = incoming[incoming[natural_key].isin(changed_nks)].copy()
    if not new_versions.empty:
        new_versions[surrogate_key] = range(next_sk, next_sk + len(new_versions))
        new_versions["valid_from"] = load_ts
        new_versions["valid_to"] = _FAR_FUTURE
        new_versions["is_current"] = True
        next_sk += len(new_versions)
        result = pd.concat([result, new_versions], ignore_index=True)

    # insert brand-new records 
    existing_nks: set[object] = set(existing[natural_key])
    brand_new: pd.DataFrame = incoming[~incoming[natural_key].isin(existing_nks)].copy()
    if not brand_new.empty:
        brand_new[surrogate_key] = range(next_sk, next_sk + len(brand_new))
        brand_new["valid_from"] = load_ts
        brand_new["valid_to"] = _FAR_FUTURE
        brand_new["is_current"] = True
        result = pd.concat([result, brand_new], ignore_index=True)
        logger.debug("SCD2 – inserted %s new records", len(brand_new))

    return result
