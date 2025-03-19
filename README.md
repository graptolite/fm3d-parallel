For Linux systems.

# Before Usage
Place run_fm3d.py into the directory containing the FMTOMO binaries. Open run_fm3d.py and declare this directory as well on line 25. Set this file to be executable.

# Usage

In a folder that contains the files: `sources.in`, `sourcesref.in`, `receivers.in`, `invert3d.in`, `frechgen.in`, `interfaces.in`, `interfacesref.in`, `vgrids.in`, `vgridsref.in`, `propgrid.in`, `mode_set.in`, `frechet.in` (plus the following if teleseismic: `ak135.hed`, `ak135.tbl`), execute `run_fm3d.py <n_cores>` where the n_cores is the number of cores to distribute fm3d over.
