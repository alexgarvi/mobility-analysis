from extractor.extractor import Extractor

def main():
    INPUT_PATH = "data"
    DATALAKE_PATH = "datalake"
    BRONZE_PATH = DATALAKE_PATH + "/bronze"

    extractor = Extractor(
        input_path=INPUT_PATH,
        bronze_path=BRONZE_PATH
    )

    ine_files = ["empleo", "pib", "poblacion"]

    for file in ine_files:
        extractor.extract_ine(file)

    print("INE extraction completed.")

    mitma = ["poblacion", "relacion_ine_zonificacionMitma"]

    for file in mitma:
        extractor.extract_mitma(file)

    types = ["distritos", "gau", "municipios"]
    entities = ["viajes", "personas", "pernoctaciones"]

    for entity in entities:
        for type in types:
            extractor.extract_mitma_temporal(entity, type)

    print("MITMA extraction completed.")


if __name__ == "__main__":
    main()