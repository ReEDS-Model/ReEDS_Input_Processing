import numpy as np

def get_most_interior_point(poly, step_meters=1000):
    """
    Sequentially inward-buffer a polygon until it disappears, then keep the centroid
    of the penultimate iteration (thus identifying a "most interior" point).
    This method will still fail for symmetric cases (like a donut), but should work
    for many geometries.
    If the resulting point is outside the geometry (thus not interior), an exception is raised.

    Settings for testing:
    - poly = dfmap['r'].loc['p18','geometry']
    """
    bounds = poly.bounds
    ## max((xmax - xmin), (ymax - ymin))
    max_buffer = max((bounds[2] - bounds[0]), (bounds[3] - bounds[1]))
    steps = np.arange(0, max_buffer+step_meters, step_meters)
    dfiter = {}
    for i, step in enumerate(steps):
        ## Reduce the size of the polygon until it's empty
        dfiter[i] = poly.buffer(-step)
        if dfiter[i].is_empty:
            for steps_back in range(1, 101):
                ## Now step back and take the centroid
                point = dfiter[i-steps_back].centroid
                if point.within(poly):
                    return point
            ## At this point there's no good match
            err = (
                f"Most interior point is exterior after {step:.0f}-meter "
                "interior buffer; use a new method"
            )
            raise ValueError(err)


def get_node(poly, step_meters=1000):
    """
    Get centroid; if centroid is outside the geometry, get "most interior" point instead
    """
    point = poly.centroid
    if not point.within(poly):
        point = get_most_interior_point(poly, step_meters=step_meters)
    return point
