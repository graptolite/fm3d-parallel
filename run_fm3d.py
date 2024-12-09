#!/usr/bin/env python3

from multiprocessing import Pool
import os
import pandas as pd
import re
import numpy as np
import shutil
import subprocess
from distutils.dir_util import copy_tree
import sys

try:
    cores = int(sys.argv[1])
except:
    cores = 2

fmtomo_bin = "<path to fmtomo bin>"
try:
    sources_wd = str(sys.argv[2])
except:
    sources_wd = input("Sources working dir must be provided:\n")

def fmtomo(cmd,wd="./"):
    ''' Execute an fmtomo command.

    cmd | <str> | fmtomo command to execute.
    wd  | <str> | path to the directory in which the command is to be executed.
    '''
    # Store current directory.
    owd = os.getcwd()
    # Change working directory.
    os.chdir(wd)
    # Execute fmtomo command.
    subprocess.call([os.path.join(fmtomo_bin,cmd)])
    # Return to original directory.
    os.chdir(owd)
    return

owd = os.getcwd()
os.chdir(sources_wd)
fmtomo("obsdata")
os.chdir(owd)
shutil.copy2(os.path.join(sources_wd,"sources.in"),"sourcesref.in")

class InputFile():
    def __init__(self,path_to_infile):
        '''
        path_to_infile | <str> | path to the fmtomo input file.
        '''
        self.path = path_to_infile
        # Load content of the fmtomo input file.
        with open(path_to_infile) as infile:
            inputs = infile.read()
        # Split content into lines.
        self.lines = inputs.split("\n")
        return
    def save(self,path=None):
        ''' Save the current state of the input lines to disk.

        path | <str> | alternative file to save the input lines to (instead of the original file).

        Returns: None
        '''
        # Check whether a save path has been provided, and if not, use the original filepath.
        if not path:
            path = self.path
        # Save the input lines to the filepath.
        with open(path,"w") as outfile:
            outfile.write("\n".join(self.lines))
        return
    def update(self,update_dict):
        ''' Update the value (but keep the comment) for a specified line.

        update_dict | <dict> {<int>:<str>} | dictionary specifying the nature of the updates in the format {line number:"what to update the line with"}.

        Returns: None
        '''
        # Iterate through the lines to be updated.
        for line,repl in update_dict.items():
            # Replace the value (in front of the comment) on the active line with the new value.
            self.lines[line-1] = repl + " "*20 + "c:" + self.lines[line-1].split("c:")[-1]
        return
    def read(self,line):
        ''' Read the value from a line.

        line | <int> | line of interest.

        Returns: <str> | value on the line of interest.
        '''
        entry = self.lines[line-1].split("c:")[0]
        return entry

def identify_sources_file(obsdata_path):
    obsdata_in = InputFile(obsdata_path)
    sources_file = obsdata_in.read(16).strip()
    is_local = obsdata_in.read(17).strip() == "0"
    return sources_file,is_local

def normalize_obsdata(obsdata_path):
    obsdata_in = InputFile(obsdata_path)
    for l in [6,7,8,12]:
        path = obsdata_in.read(l).strip()
        obsdata_in.update({l:os.path.split(path)[-1]})
        obsdata_in.save()
    return

def split_sources(sources_wd,cores):
    obsdata_sources_input,is_local = identify_sources_file(os.path.join(sources_wd,"obsdata.in"))
    normalize_obsdata(os.path.join(sources_wd,"obsdata.in"))
    print(obsdata_sources_input,is_local)
    original_owd = os.getcwd()
    with open(os.path.join(sources_wd,obsdata_sources_input)) as infile:
        source_lines = [l for l in infile.read().split("\n")[1:] if l.strip()]
    source_data = []
    # Determine number of lines per source based on whether it's local or not.
    lines_per_source = 3 if is_local else 1
    for i in range(int(len(source_lines)/lines_per_source)):
        source_data.append("\n".join(source_lines[i*lines_per_source:(i+1)*lines_per_source]))

    swap_wd = os.path.join(sources_wd,".tmp")
    if not os.path.exists(swap_wd):
        os.mkdir(swap_wd)
    else:
        for f in os.listdir(swap_wd):
            shutil.rmtree(os.path.join(swap_wd,f))

    owd = sources_wd
    source_sublists = np.array_split(source_data,cores)
    wds = []
    picks_parent = os.path.join(sources_wd,"picks")
    for i,sl in enumerate(source_sublists):
        tmp_wd = os.path.join(swap_wd,str(i))
        if not os.path.exists(tmp_wd):
            os.mkdir(tmp_wd)
        for f in ["obsdata.in"]:
            if not os.path.isdir(f):
                shutil.copy2(os.path.join(sources_wd,f),os.path.join(tmp_wd,f))
        sl = list(sl)
        sl = [str(len(sl))] + sl
        with open(os.path.join(tmp_wd,obsdata_sources_input),"w") as outfile:
            outfile.write("\n".join(sl))
        picks_tmp = os.path.join(tmp_wd,"picks")
        os.mkdir(picks_tmp)
        for f in os.listdir(picks_parent):
            shutil.copy2(os.path.join(picks_parent,f),os.path.join(picks_tmp,f))
        os.chdir(tmp_wd)
        fmtomo("obsdata")
        os.chdir(owd)
        wds.append(tmp_wd)
    os.chdir(original_owd)
    return wds

def combine_arrivals(out,fs):
    counter = 1
    dfs = []
    for f in fs:
        df_tmp = pd.read_csv(f,sep=r"\s+",names=[0,1,2,3,4,5,6])
        dfs_tmp = [df for _,df in df_tmp.groupby(1)]
        for i,ev_id in enumerate(dfs_tmp):
            dfs_tmp[i][1] = counter
            counter += 1
        dfs.extend(dfs_tmp)
    df = pd.concat(dfs)
    df.to_csv(out,header=None,index=None,sep="\t")
    return

def combine_frechet(out,frechs):
    counter = 0
    all_lines = []
    for frech in frechs:
        with open(frech) as infile:
            data = infile.read().split("\n")

        lines = []
        idxs = []
        for i,l in enumerate(data):
            l_data = [x for x in l.split(" ") if x]
            if len(l_data) > 3:
                lines.append(l_data)
                idxs.append(i)
        idxs.append(len(data))

        unique_evs = []
        for i,l_data in enumerate(lines):
            ev = l_data[1]
            if ev not in unique_evs:
                unique_evs.append(ev)
                counter += 1
            l_data[1] = str(counter)
            lines[i] = l_data

        for idx,line in zip(idxs,lines):
            data[idx] = "\t".join(line)
        all_lines.extend(data)

    with open(out,"w") as outfile:
        outfile.write("\n".join(all_lines))
    return

print("running on",cores,"cores")

pick_wds = split_sources(sources_wd,cores)

active_dir = ".tmp"
if not os.path.exists(active_dir):
    os.mkdir(active_dir)
for f in os.listdir(active_dir):
    shutil.rmtree(os.path.join(active_dir,f))

def execute(i):
    working_dir = os.path.join(active_dir,str(i))
    os.mkdir(working_dir)
    # Copy grids.
    files = ["frechgen.in","frechet.in","interfaces.in","propgrid.in","vgrids.in","mode_set.in","ak135.hed","ak135.tbl"]
    for f in files:
        try:
            shutil.copy2(f,working_dir)
        except:
            print("Failed to find file",f)
    shutil.copy2("interfaces.in",os.path.join(working_dir,"interfacesref.in"))
    shutil.copy2("vgrids.in",os.path.join(working_dir,"vgridsref.in"))
    files = ["sources.in","receivers.in","otimes.dat"]
    for f in files:
        shutil.copy2(os.path.join(pick_wds[i],f),working_dir)
    owd = os.getcwd()
    os.chdir(working_dir)
    fmtomo("fm3d")
    os.chdir(owd)
    return

def parallel(f,cores,args):
    with Pool(cores) as p:
        out = p.map(f,range(args))
    print("Finished")
    return

parallel(execute,cores,cores)

try:
    os.remove("arrivals.dat")
except FileNotFoundError:
    pass
arrival_fs = [os.path.join(active_dir,str(i),"arrivals.dat") for i in range(cores)]
frechet_fs = [os.path.join(active_dir,str(i),"frechet.dat") for i in range(cores)]
combine_arrivals("arrivals.dat",arrival_fs)
combine_frechet("frechet.dat",frechet_fs)
