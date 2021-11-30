import glob
from pathlib import Path

import iris
import iris.pandas
from esmvalcore import preprocessor
from pathos.threading import ThreadPool as Pool

# Set Paths (Shown are for Snellius HPC)
SHAPE_DIR = "/gpfs/home6/jaerts/fransje_code/catchment_shapefiles/"
NC4_DIR = "/gpfs/home6/jaerts/fransje_code/GSWP3-data/"
OUT_DIR = "/gpfs/home6/jaerts/fransje_code/output/"

def area_weighted_shapefile_rasterstats(
    catchment_shapefile,
    catchment_netcdf,
    statistical_operator,
    output_dir,
    output_csv=False,
    return_cube=False,
):
    """
    Calculate area weighted zonal statistics of netcdfs using a shapefile to extract netcdf data.

    catchment_shapefile:  str, catchment shapefile
    catchment_netcdf:     str, netcdf file
    statistical_operator: str, (mean (NOT area weighted), median (NOT area weighted), sum, variance, min, max, rms)
    - https://docs.esmvaltool.org/projects/esmvalcore/en/latest/api/esmvalcore.preprocessor.html#esmvalcore.preprocessor.area_statistics
    output_csv:          bool, True stores csv output and False stores netcdf output

    Returns: iris cube, stores .csv file or .nc file
    """
    # Load iris cube of netcdf
    cube = iris.load_cube(catchment_netcdf)

    # From cube extract shapefile shape
    cube = preprocessor.extract_shape(cube, catchment_shapefile)

    # Calculate area weighted statistics
    cube_stats = preprocessor.area_statistics(cube, statistical_operator)

    if output_csv is True:
        # Convert cube to dataframe
        df = iris.pandas.as_data_frame(cube_stats)

        # Change column names
        df = df.reset_index()
        df = df.set_axis(["time", cube_stats.name()], axis=1)

        # Write csv as output
        df.to_csv(
            f"{output_dir}/{Path(catchment_shapefile).name.split('.')[0]}_{catchment_netcdf.split('/')[-1].split('_')[0]}_{statistical_operator}.csv"
        )
    else:
        iris.save(
            cube_stats,
            f"{output_dir}/{Path(catchment_shapefile).name.split('.')[0]}_{catchment_netcdf.split('/')[-1].split('_')[0]}_{statistical_operator}.nc",
        )

    if return_cube == True:
        return cube
    else:
        return

def run_function_parallel(
    shapefile_list=list,
    netcdf_list=list,
    operator_list=list,
    output_dir_list=list,
    threads=None,
):
    """
    Runs function area_weighted_shapefile_rasterstats in parallel.

    shapefile_list:  str, list, list of input catchment shapefiles
    netcdf_list:     str, list, list of input netcdf files
    operator_list:   str, list, list of statistical operators (single operator)
    output_dir_list: str, list, list of output directories
    threads:         int,       number of threads (cores), when set to None use all available threads

    Returns: None
    """
    # Set number of threads (cores) used for parallel run and map threads
    if threads is None:
        pool = Pool()
    else:
        pool = Pool(nodes=threads)
    # Run parallel models
    results = pool.map(
        area_weighted_shapefile_rasterstats,
        shapefile_list,
        netcdf_list,
        operator_list,
        output_dir_list,
    )

    return results

def construct_lists_for_parallel_function(NC4_DIR, SHAPE_DIR, OUT_DIR):
    """
    This functions constructs list for running code in parallel.

    NC4_DIR:              str, dir containing input netcdf files for area weighted statitic calculations
    SHAPE_DIR:            str, dir containing catchment shapefiles
    OUT_DIR:              str, dir for output storage

    Returns: list of netcdfs, shapefiles, output directories
    """
    netcdfs = glob.glob(f"{NC4_DIR}/*nc")
    shapefiles = glob.glob(f"{SHAPE_DIR}/*shp")

    output_dir = [OUT_DIR]

    shapefile_list = shapefiles * len(netcdfs)
    shapefile_list.sort()
    netcdf_list = netcdfs * len(shapefiles)
    output_dir_list = output_dir * len(shapefile_list)

    operator_list = []

    for netcdf in netcdf_list:
        if "tas" in netcdf:
            operator_list.append("mean")
        else:
            operator_list.append("sum")

    return shapefile_list, netcdf_list, operator_list, output_dir_list

# Construct lists for parallel run
(
    shapefile_list,
    netcdf_list,
    operator_list,
    output_dir_list,
) = construct_lists_for_parallel_function(NC4_DIR, SHAPE_DIR, OUT_DIR)

%%time
# Test speed of parallel run
run_function_parallel(shapefile_list, netcdf_list, operator_list, output_dir_list)