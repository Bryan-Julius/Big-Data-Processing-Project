import os
import xarray as xr
from pyspark.sql import Row
import pyproj

def extract_features(file_path, lat_decimal, lon_decimal):
    """
    Opens a NetCDF file, calculates the bounding box around the hurricane's
    center coordinates, crops the array, and extracts statistical features.
    """
    filename = os.path.basename(file_path)

    try:
        # Open dataset
        ds = xr.open_dataset(file_path, engine='netcdf4')

        # Extract satellite projection metadata
        proj_info = ds.goes_imager_projection
        perspective_height = proj_info.perspective_point_height

        # Setup the coordinate transformer (Standard Lat/Lon -> GOES Camera Radians)
        p_goes = pyproj.Proj(
            proj='geos', h=perspective_height, lon_0=proj_info.longitude_of_projection_origin,
            sweep=proj_info.sweep_angle_axis, a=proj_info.semi_major_axis, b=proj_info.semi_minor_axis
        )
        p_latlon = pyproj.Proj(proj='latlong', datum='WGS84')
        transformer = pyproj.Transformer.from_proj(p_latlon, p_goes)

        # Calculate bounding box (± 5 degrees from storm center)
        # Convert Lat/Lon to GOES projection meters
        min_x, max_y = transformer.transform(lon_decimal - 5, lat_decimal + 5)
        max_x, min_y = transformer.transform(lon_decimal + 5, lat_decimal - 5)

        # Convert projection meters to camera radians for xarray slicing
        min_x_rad, max_x_rad = min_x / perspective_height, max_x / perspective_height
        min_y_rad, max_y_rad = min_y / perspective_height, max_y / perspective_height

        # Crop the tensor to the bounding box
        cropped_ds = ds.sel(
            x=slice(min_x_rad, max_x_rad),
            y=slice(max_y_rad, min_y_rad)
        )

        # Extract features from ONLY the cropped hurricane area
        cmi_tensor = cropped_ds['CMI']
        mean_radiance = float(cmi_tensor.mean().values)
        max_radiance = float(cmi_tensor.max().values)

        ds.close()

        return Row(
            filename=filename,
            mean_radiance=mean_radiance,
            max_radiance=max_radiance
        )

    except Exception as e:
        # If the math fails (e.g., storm is off the edge of the satellite image)
        print(f"Error processing {filename}: {e}")
        return Row(filename=filename, mean_radiance=-1.0, max_radiance=-1.0)