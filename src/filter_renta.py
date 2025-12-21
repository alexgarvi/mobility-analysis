import json

def filtrar_renta(id_metadata_1, id_metadata_3):
    """
    Filtra por IDs de MetaData y devuelve una lista plana donde cada elemento
    tiene exactamente las claves: Codigo, Fecha y Valor.
    """
    resultados = []

    with open("./data/ine/30656.json") as f:
        datos = json.load(f)
    for item in datos:

        meta = item.get("MetaData", [])
        
        # Verificamos que existan los metadatos y coincidan los IDs
        if len(meta) >= 3:
            if meta[0].get("T3_Variable") == id_metadata_1 and meta[2].get("Nombre") == id_metadata_3:
                
                # Obtenemos el código común para este bloque
                codigo = meta[0].get("Codigo")
                
                # Recorremos cada dato y creamos un objeto individual
                for dato in item.get("Data", []):
                    registro = {
                        "Codigo": codigo,
                        "Fecha": dato.get("Fecha"),
                        "Valor": dato.get("Valor")
                    }
                    resultados.append(registro)
    
    with open("./data/ine/renta_filtrada.json", "w") as f:
        f.write(json.dumps(resultados, indent=4))
    #return json.dumps(resultados, indent=4)

# --- Ejemplo de uso ---
# Suponiendo que 'tu_lista_de_datos' es la variable que contiene el JSON original
# Filtramos por Id 6124 (Municipio) y Id 284048 (Renta)

# json_resultado = filtrar_y_aplanar(tu_lista_de_datos, 6124, 284048)
# print(json_resultado)

if __name__ == "__main__":
    filtrar_renta("Secciones", "Renta neta media por persona")