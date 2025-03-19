For Linux systems.

# Before Usage
Place run_fm3d.py into the directory containing the FMTOMO binaries. Open run_fm3d.py and declare this directory as well on line 25. Set this file to be executable.

# Usage

In a folder that contains the files: `sources.in`, `sourcesref.in`, `receivers.in`, `invert3d.in`, `frechgen.in`, `interfaces.in`, `interfacesref.in`, `vgrids.in`, `vgridsref.in`, `propgrid.in`, `mode_set.in`, `frechet.in` (plus the following if teleseismic: `ak135.hed`, `ak135.tbl`), execute `run_fm3d.py <n_cores>` where the n_cores is the number of cores to distribute fm3d over.

Note: `invert3d.in` is required for `frechgen.in`. No inversion in run in this code.

# Usage with tomo3d

Steps:
1. Explicitly add the path to the fmtomo binaries.
2. Add code to pass on the number of cores to distribute fm3d over.
3. Replace the value of the `fmm` variable with `run_fm3d.py` followed by the number of cores.

E.g.

Changing the start of tomo3d<sup>(1)</sup>:

<details>
<summary><b><u>Original</u></b></summary>
```bash
#!/usr/bin/ksh
#
# NOTE: If ksh is not available, you may use zsh.
#
############################################
# Script for running multi-parameter
# tomography program
############################################
#
########################################################
# Program and files for solving the forward problem with
# the Fast Marching Method
########################################################
#
# Name of program for calculating FMM traveltimes
fmm=fm3d
```
</details>

<details open>
<summary><b><u>Updated</u></b></summary>
```bash
#!/usr/bin/ksh
#
# NOTE: If ksh is not available, you may use zsh.
#

PATH=$PATH:&lt;path to folder fmtomo binaries&gt;
############################################
# Script for running multi-parameter
# tomography program
############################################
#
########################################################
# Program and files for solving the forward problem with
# the Fast Marching Method
########################################################
#
ncores_default=2
ncores="${1:-$ncores_default}"
echo "Running on $ncores cores"
# Name of program for calculating FMM traveltimes
fmm="run_fm3d.py $ncores"
```
</details>

(1) Original script from https://github.com/nrawlinson/FMTOMO.
