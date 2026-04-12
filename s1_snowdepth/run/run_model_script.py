import joblib
import xarray as xr
import sys
from glob import glob
import numpy as np

import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import pandas.core.indexes.base as base
sys.modules['pandas.core.index.numeric'] = base
base.Int64Index = pd.Index
base.Float64Index = pd.Index


from s1_snowdepth.config import Config
from s1_snowdepth.run.ml_snow_functions import prep_data, reproject_m

### ------ UNIVERSAL VARIABLES ------ #####
numeric_features = ['elevation', 'slope', 'aspect', 'fcf', 'tpi', 'DayOfSeason', 'vv_scaled', 'cr_scaled', 'lia',
                    'sc_percum', 'sc_per']
categorical_features = ['snowclass']
all_features = numeric_features.copy()
all_features.extend(categorical_features)

def run(cfg: Config):
    static_var = xr.open_dataset(cfg.static_var_path).transpose('lat', 'lon')
    snow_classes = xr.open_dataset(cfg.snow_class_path).transpose('lat', 'lon')
    landcover = xr.open_dataset(cfg.landcover_path).transpose('lat', 'lon')
    static_var['snow_class'] = snow_classes['class']
    static_var['lc'] = landcover['lc']
    static_var.rio.write_crs("EPSG:4326", inplace=True)
    grid = xr.open_dataset(cfg.grid_path)
    glaciers = xr.open_dataset(cfg.glacier_path)

    model = joblib.load(cfg.model_path / f'{cfg.model_version}.pkl')

    s1_files = glob(str(cfg.s1_mosaic_dir / '*.nc'))

    output_folder = cfg.output_dir
    output_folder.mkdir(parents=True, exist_ok=True)
    finished_files = glob(str(output_folder / '*.nc'))
    print(finished_files)


    ##### ------ CODE ------ #####

    for f in s1_files:
        date = f.split('_')[-3]
        month = date[4:6]
        ym = date[0:6]
        orbit = f.split('_')[-2]
        year = date[0:4]
        print(date, orbit, year)

        out_file = f'{output_folder}/S1_ml_SD_{date}_{orbit}.nc'

        if (out_file not in finished_files and orbit not in cfg.skip_orbits and month not in cfg.skip_months and ym not in
                ['201501','201502','201503','201504','201505']):

            print('prepping data')
            all_var, df_x = prep_data(date, orbit, static_var, cfg)

            df_nonan = df_x[all_features]
            df_nonan = df_nonan.dropna()  # .reset_index(drop = True)

            print('running model')
            if len(df_nonan) > 0:
                df_nonan['SD'] = model.predict(df_nonan)
                df_x.loc[df_nonan.index, 'SD'] = df_nonan['SD']
                df_x.loc[df_x.SD < 0, 'SD'] = 0

            SD = df_x.SD.values.reshape(all_var.cr_scaled.values.shape)
            SD[(all_var.lc == 80) | (all_var.lc == 200)] = np.nan
            all_var = all_var.assign(SD=(['lat', 'lon'], SD))

            print('saving')
            sd = all_var.SD  # .drop(['TPI','slope','aspect','dem','forest','grid','snow_class','lc'])
            glaciers_crop = glaciers.sel(lat=slice(sd.lat.max().values, sd.lat.min().values),
                                         lon=slice(sd.lon.min().values, sd.lon.max().values))
            sd = reproject_m(sd, glaciers_crop)
            sd = sd.where(glaciers_crop.glacier != 1)
            sd.to_netcdf(out_file)

            # all_var.to_netcdf(f'{out_file}')
            # with open(f'{output_folder}p_{date}_{orbit}.p', 'wb') as fp:
            # pickle.dump(df_x, fp)

        else:
            print('Already done', date, orbit)