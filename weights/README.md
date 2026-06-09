# Generate a weights csv file to blur fuel weights across regions

`generate_weights.py` maps counties to census divisions based on the distance from the county centroid to the census division border.
Counties near census division boundaries end up being defined by a mix of census divisions, avoiding sharp spatial jumps in parameters defined by census division (like gas price).
An exponential decay with a characteristic length of 150 km is used by default.
