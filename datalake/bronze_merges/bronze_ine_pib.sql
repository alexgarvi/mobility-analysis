MERGE INTO bronze_ine_pib AS target
USING source_query as source
    ON target.province = source.province,
    AND target.branch = source.branch,
    AND target.period = source.period

WHEN MATCHED THEN
    UPDATE SET
        target.total = source.total

WHEN NOT MATCHED THEN 
    INSERT (
        province,
        branch,
        period,
        total
    )
    VALUES (
        source.province,
        source.branch,
        source.period,
        source.total
    )
