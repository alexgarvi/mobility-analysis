MERGE INTO bronze_ine_pib AS target
USING source_query as source
    ON target.total_nat = source.total_nat,
    AND target.province = source.province,
    AND target.municipality = source.municipality,
    AND target.gender = source.gender,
    AND target.age = source.age,
    AND target.nationality = source.nationality,
    AND target.year = source.year

WHEN MATCHED THEN
    UPDATE SET
        target.total = source.total

WHEN NOT MATCHED THEN 
    INSERT (
        total_nat,
        province,
        municipality,
        gender,
        age,
        nationality,
        year,
        total
    )
    VALUES (
        source.total_nat,
        source.province,
        source.municipality,
        source.gender,
        source.age,
        source.nationality,
        source.year,
        source.total
    )
