from pathlib import Path

from s1_snowdepth.config import Config
from s1_snowdepth.preprocessing.snow_cover_fraction import (load_ims, load_modis, gap_fill_modis,
                                                            compute_scf, compute_cumulative_scf)
from s1_snowdepth.download.ims import download_ims
from s1_snowdepth.download.modis import download_modis

def test_load_ims():
    cfg = Config()

    date = "20190301"
    ims_dir = Path(__file__).parent.parent / "data" / "ims"
    ims_path = ims_dir / f"ims_{date}_1km.nc"

    if not ims_path.exists():
        print(f"No IMS file found for {date}, downloading...")
        download_ims(date, ims_dir, cfg)

    ims = load_ims(ims_path, cfg)

    # Convert to tif
    # ims.rio.to_raster(ims_path.with_suffix(".tif"))

    n_snow_pixels = int(ims.sum())
    print(f"IMS shape: {ims.shape}\n"
          f"    CRS: {ims.rio.crs}\n"
          f"    snow pixels: {n_snow_pixels}\n"
          f"    dims: {ims.dims}\n"
    )

    assert (n_snow_pixels > 0)


def test_load_modis():
    cfg = Config()

    date = "20190301"
    modis_dir = Path(__file__).parent.parent / "data" / "modis"
    modis_path = modis_dir / f"modis_scf_{date}.tif"

    if not modis_path.exists():
        print(f"No MODIS file found for {date}, downloading...")
        download_modis(date, modis_dir, cfg)

    modis = load_modis(modis_path, cfg)

    n_null_pixels = int(modis.isnull().sum())
    print(f"MODIS shape: {modis.shape}\n"
          f"      CRS: {modis.rio.crs}\n"
          f"      min: {float(modis.min()):.3f}\n"
          f"      max: {float(modis.max()):.3f}\n"
          f"      null pixels: {n_null_pixels}\n"
          f"      dims: {modis.dims}\n"
    )

    assert n_null_pixels != int(modis.count())


def test_gap_fill_modis():
    cfg = Config()

    date = "20190301"
    modis_dir = Path(__file__).parent.parent / "data" / "modis"

    modis = load_modis(modis_dir / f"modis_scf_{date}.tif", cfg)
    gap_filled = gap_fill_modis(date, modis_dir, cfg)

    null_before = int(modis.isnull().sum())
    null_after = int(gap_filled.isnull().sum())
    print(f"Gap-filled shape: {gap_filled.shape}\n"
          f"           min: {float(gap_filled.min()):.3f}\n"
          f"           max: {float(gap_filled.max()):.3f}\n"
          f"           null pixels before: {null_before}\n"
          f"           null pixels after: {null_after}\n"
    )

    assert (null_before == 0) or (null_after < null_before)


def test_compute_scf():
    cfg = Config()

    date = "20190301"
    ims_dir = Path(__file__).parent.parent / "data" / "ims"
    modis_dir = Path(__file__).parent.parent / "data" / "modis"

    scf = compute_scf(date, ims_dir, modis_dir, cfg)
    print(f"SCF shape: {scf.shape}\n"
          f"    CRS: {scf.rio.crs}\n"
          f"    min: {float(scf.min()):.3f}\n"
          f"    max: {float(scf.max()):.3f}\n"
          f"    null pixels: {int(scf.isnull().sum())}\n"
    )

    # Convert to raster
    output_dir = Path(__file__).parent.parent / "data" / "snow_cover"
    output_dir.mkdir(parents=True, exist_ok=True)
    scf.rio.to_raster(str(output_dir / f"scf_{date}.tif"))

    assert float(scf.min()) >= 0 and float(scf.max()) <= 1


def test_compute_cumulative_scf():
    # This is a poor test to avoid downloading lots of data. Should be done using mock data
    cfg = Config()

    date = "20180802"
    ims_dir = Path(__file__).parent.parent / "data" / "ims"
    modis_dir = Path(__file__).parent.parent / "data" / "modis"

    cscf = compute_cumulative_scf(date, ims_dir, modis_dir, cfg)

    assert float(cscf.sc_percum.mean()) >= float(cscf.sc_per.mean())


if __name__ == "__main__":
    # test_load_ims()
    # test_load_modis()
    # test_gap_fill_modis()
    # test_compute_scf()
    test_compute_cumulative_scf()