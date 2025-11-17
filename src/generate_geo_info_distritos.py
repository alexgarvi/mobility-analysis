import geopandas as gpd
import pandas as pd

gdf = gpd.read_file("./data/mitma/zonificacion/distritos/zonificacion_distritos_centroides.shp")
gdf['lon'] = gdf.get_coordinates().x
gdf['lat'] = gdf.get_coordinates().y

df = pd.DataFrame(gdf)
df.drop('geometry', axis=1, inplace=True)
df.to_csv('./data/mitma/zonificacion/distritos/distritos_centroides.csv', sep='|', index=False)