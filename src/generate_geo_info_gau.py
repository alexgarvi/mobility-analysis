import geopandas as gpd
import pandas as pd

gdf = gpd.read_file("./data/mitma/zonificacion/gau/zonificacion_gaus_centroides.shp")
gdf['lon'] = gdf.get_coordinates().x
gdf['lat'] = gdf.get_coordinates().y

df = pd.DataFrame(gdf)
df.drop('geometry', axis=1, inplace=True)
df.to_csv('./data/mitma/zonificacion/gau/gaus_centroides.csv', sep='|', index=False)