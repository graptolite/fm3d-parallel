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

def modify_receiver_source(receiver_data,new_source):
    receiver_l = receiver_data.split("\n")
    receiver_l[2] = "           %u" % int(new_source)
    return "\n".join(receiver_l)

def load_receiver_dict(receivers_file="receivers.in"):
    # receivers.in
    # Skip the first line.
    # Any line containing a decimal point is the start of a receiver block.
    with open(receivers_file) as infile:
        lines = [l for l in infile.read().split("\n")[1:] if l.strip()]
    receiver_starts = []
    for i,l in enumerate(lines):
        # A decimal point in a line means it locates a source.
        if "." in l:
            receiver_starts.append(i)
    receiver_starts.append(len(lines))
    receivers = ["\n".join(lines[receiver_starts[i]:receiver_starts[i+1]]) for i in range(len(receiver_starts)-1)]
    receiver_dict = dict()
    for r in receivers:
        # The third line will always be the source id.
        source_id = int(r.split("\n")[2].strip())
        if source_id in receiver_dict:
            receiver_dict[source_id].append(r)
        else:
            receiver_dict[source_id] = [r]
    return receiver_dict

def load_sources_list(sources_file="sources.in"):
    # sources.in
    # Skip the first line.
    # Any line containing a decimal point is the line after the first of a source block.
    # Any prior line containing no number (i.e. letter phase) shifts the starting line of the block back one.
    with open(sources_file) as infile:
        lines = [l for l in infile.read().split("\n")[1:] if l.strip()]
    source_starts = []
    for i,l in enumerate(lines):
        # A decimal point in a line means it locates a source, which is the second line after the start of a source block.
        if "." in l:
            if not re.search("[0-9]",lines[i-1]):
                source_starts.append(i-2)
            else:
                source_starts.append(i-1)
    source_starts.append(len(lines))
    sources = [(i+1,"\n".join(lines[source_starts[i]:source_starts[i+1]])) for i in range(len(source_starts)-1)]
    return sources

def split_sources(cores):
    tmp = ".tmp"
    if not os.path.exists(tmp):
        os.mkdir(tmp)
    receiver_dict = load_receiver_dict()
    sources = load_sources_list()
    distrib_s = np.array_split(sources,cores)
    wds = []
    for i,sub_s in enumerate(distrib_s):
        t_wd = os.path.join(tmp,str(i))
        if not os.path.exists(t_wd):
            os.mkdir(t_wd)
        with open(os.path.join(t_wd,"sources.in"),"w") as outfile:
            outfile.write("\n".join([str(len(sub_s))] + [x[1] for x in sub_s]))
        source_ids = [int(x[0]) for x in sub_s]
        norm_source_ids = np.array(source_ids) - source_ids[0] + 1
        active_receivers = []
        for s_id,n_s_id in zip(source_ids,norm_source_ids):
            s_receivers = receiver_dict[s_id]
            if s_id != n_s_id:
                s_receivers = [modify_receiver_source(r,n_s_id) for r in s_receivers]
            active_receivers.extend(s_receivers)
        with open(os.path.join(t_wd,"receivers.in"),"w") as outfile:
            outfile.write("\n".join([str(len(active_receivers))] + active_receivers))
        wds.append(t_wd)
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

active_dir = ".tmp"
if not os.path.exists(active_dir):
    os.mkdir(active_dir)
for f in os.listdir(active_dir):
    shutil.rmtree(os.path.join(active_dir,f))

pick_wds = split_sources(cores)

def execute(i):
    working_dir = os.path.join(active_dir,str(i))
    if not os.path.exists(working_dir):
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
