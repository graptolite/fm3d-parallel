#!/usr/bin/env python3

from multiprocessing import Pool
import os
import pandas as pd
import re
import numpy as np
import shutil
import subprocess
import sys

# Read the number of cores for parallel execution of fm3d, defaulting to 2 cores if no input provided or the input provided is in an unsuitable format.
try:
    cores = int(sys.argv[1])
except:
    cores = 2

############################################
# TO BE MODIFIED ON A PER-SYSTEM BASIS     #
# Path to the fmtomo binaries/executables. #
fmtomo_bin = "<path to fmtomo bin>"
############################################

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

def check_source_inversion():
    ''' Read invert3d.in to determine whether sources are to be inverted for (which affects what files need to be pushed to each core's subdirectory).

    Returns: <bool> | whether source inversion is turned on.
    '''
    try:
        with open("invert3d.in") as infile:
            lines = infile.read().split("\n")
        # Read status of source inversion switch (either "0" or "1").
        source_inversion = lines[24].split(" ")[0]
        return bool(int(source_inversion))
    except FileNotFoundError:
        # Check whether uncertainties are provided in sourcesref.in for relocation.
        with open("sourcesref.in") as infile:
            first_source = infile.read().split("\n")[2]
        data = [x for x in first_source.split(" ") if x.strip()]
        return (len(data) > 3)

def modify_receiver_source(receiver_data,new_source):
    ''' Replace the source id (integer) inside some receiver data with a new source id.

    receiver_data | <str>      | fmtomo format receiver data from receiver.in.
    new_source    | <int>-like | source id that can be cast to an integer to replace the pre-existing source id associated with the input receiver data with.

    Returns: <str> receiver data updated with the new source.
    '''
    receiver_l = receiver_data.split("\n")
    receiver_l[2] = "           %u" % int(new_source)
    return "\n".join(receiver_l)

def load_receiver_dict(receivers_file="receivers.in"):
    ''' Parse the relation between receivers and source to create a dictionary mapping source ids to a block of receivers data (for all receivers that picked up that source).

    receivers_file | <str> | name of the file containing all receivers data.

    Returns: <dict> {<str>:[<str>]} | dictionary in the format {<source id>:<receivers data list>} source id is a string representation of the integer source id and the receivers data list is a list of receiver data in fmtomo format. The data for each receiver contains the receiver location.
    '''
    with open(receivers_file) as infile:
        # Read full receivers data into a list of lines then skip the first line which just gives the number of receivers.
        lines = [l for l in infile.read().split("\n")[1:] if l.strip()]
    # Initialize list to hold the line indices representing the start of a data block for one receiver.
    receiver_starts = []
    # Iterate through the lines of receiver data.
    for i,l in enumerate(lines):
        # If a decimal point is present in a line (i.e. the line doesn't just comprise integers), the line represents the location of a receiver and therefore starts a receiver's data block.
        if "." in l:
            receiver_starts.append(i)
    # Add the length of the list of data lines to permit collection of the lines into per-receiver groups.
    receiver_starts.append(len(lines))
    # Segment the lines into per-receiver data blocks.
    receivers = ["\n".join(lines[receiver_starts[i]:receiver_starts[i+1]]) for i in range(len(receiver_starts)-1)]
    # Initialize dictionary to store the mapping between sources and the receivers that detected them.
    receiver_dict = dict()
    # Iterate through all receivers in the data.
    for r in receivers:
        # Identify the source that the receiver picked up using the third line (of each receiver data block).
        source_id = int(r.split("\n")[2].strip())
        # Add the receiver to the list of receivers that picked up this source.
        if source_id in receiver_dict:
            receiver_dict[source_id].append(r)
        else:
            receiver_dict[source_id] = [r]
    return receiver_dict

def load_sources_list(sources_file="sources.in"):
    ''' Parse the sources specification file into a list of individual sources specifications.

    sources_file | <str> | name of the file containing all sources data.

    Returns: <list> [<str>] | list of individual source data.
    '''
    with open(sources_file) as infile:
        # Read full sources data into a list of lines then skip the first line which just gives the number of sources.
        lines = [l for l in infile.read().split("\n")[1:] if l.strip()]
    # Initialize list to hold the line indices representing the start of a data block for one source.
    source_starts = []
    # Iterate through the lines of sources data.
    for i,l in enumerate(lines):
        # If a decimal point is present in a line (i.e. the line doesn't just comprise integers), then it locates a source.
        if "." in l:
            # If the line above the source doesn't contain a number, then it is a phase definition, and two lines above the location line represents the start of the source (i.e. denotes it's a teleseismic source).
            # I.e. the format is:
            #      1
            #      <phase>
            #      <location>
            #      <path ...>
            # This could probably be simplified to if lines[i-1].strip() != "0"...
            if not re.search("[0-9]",lines[i-1]):
                source_starts.append(i-2)
            else:
                # Otherwise the line above denotes a local source and so just one line above the location line represents the start of the source.
                # I.e. the format is:
                #      0
                #      <location>
                #      <path ...>
                source_starts.append(i-1)
    # Add the length of the list of data lines to permit collection of the lines into per-source groups.
    source_starts.append(len(lines))
    # Segment the lines into per-source data blocks.
    sources = [(i+1,"\n".join(lines[source_starts[i]:source_starts[i+1]])) for i in range(len(source_starts)-1)]
    return sources

def split_sources(cores,tmp=".tmp"):
    ''' Divide the sources described in sources.in roughly evenly across a number of cores by placing in temporary subfolders.

    cores | <int> | number of subfolders (cores) to divide the contents of sources.in across.
    tmp   | <str> | path to "temporary" folder whose contents do not need to be saved after fm3d is complete. Contains the subfolders that hold divided sources.

    Returns: <list> [<str>] | list of paths to the subfolders over which the sources are distributed.
    '''
    # Ensure the temporary folder exists.
    if not os.path.exists(tmp):
        os.mkdir(tmp)
    # Load dictionary holding relation between sources and the receivers that picked them up.
    receiver_dict = load_receiver_dict()
    # Load list of sources and sourcesref (described by data blocks).
    sources = load_sources_list()
    sources_ref = load_sources_list("sourcesref.in")
    # Prevent cores exceeding the number of sources.
    if len(sources) < cores:
        cores = len(sources)
        print("Reduced cores to",cores)
    if len(sources) != len(sources_ref):
        raise ValueError("Sources doesn't match sources ref")
    # Split the list of sources and reference sources into roughly equal length sublists, with the number of sublists being equal to the number of cores.
    distrib_s = np.array_split(sources,cores)
    distrib_sref = np.array_split(sources_ref,cores)
    # Initialize list to hold paths to subfolders over which source distribution happens.
    wds = []
    # Iterate through the sublists.
    for i,(sub_s,sub_sr) in enumerate(zip(distrib_s,distrib_sref)):
        # Ensure the presence of a subfolder corresponding to the active sublist index, which corresponds to a core index (unique but disordered).
        t_wd = os.path.join(tmp,str(i))
        if not os.path.exists(t_wd):
            os.mkdir(t_wd)
        # Write the subfolder's sources.in and sourcesref.in containing just the sources in the active sources sublist.
        with open(os.path.join(t_wd,"sources.in"),"w") as outfile:
            outfile.write("\n".join([str(len(sub_s))] + [x[1] for x in sub_s]))
        with open(os.path.join(t_wd,"sourcesref.in"),"w") as outfile:
            outfile.write("\n".join([str(len(sub_sr))] + [x[1] for x in sub_sr]))
        # Extract source ids for all sources in the active sources sublist.
        source_ids = [int(x[0]) for x in sub_s]
        # Create a normalized, sublist-independent list of source ids (i.e. starting at 1, only for the sources within the active source sublist)
        norm_source_ids = np.array(source_ids) - source_ids[0] + 1
        # Initialize list to hold an ordered receivers list corresponding to the active sources.
        active_receivers = []
        # Iterate through the canonical source ids as well as their normalized equivalent.
        for s_id,n_s_id in zip(source_ids,norm_source_ids):
            # Identify the receivers corresponding to the canonical source id (which the receivers are indexed by).
            s_receivers = receiver_dict[s_id]
            # If the normalized source id is not the same as the canonical source id, then modify the receiver data to ensure the receivers point to the normalized source id (i.e. are made independent from the other sublists).
            if s_id != n_s_id:
                s_receivers = [modify_receiver_source(r,n_s_id) for r in s_receivers]
            # Store the receivers for the active source to the ordered list of all receivers for the active sublist of sources.
            active_receivers.extend(s_receivers)
        # Write the subfolder's receivers.in containing just the receivers corresponding to sources in the active sources sublist.
        with open(os.path.join(t_wd,"receivers.in"),"w") as outfile:
            outfile.write("\n".join([str(len(active_receivers))] + active_receivers))
        # Store the path of the subfolder.
        wds.append(t_wd)
    return wds,cores

def combine_arrivals(out,fs):
    ''' Combine data in multiple ordered `arrivals.dat` files and save to disk.

    out | <str>          | path to the file to store the combined output in.
    fs  | <list> [<str>] | ordered list of paths to the `arrivals.dat` files whose contents are to be combined.
    '''
    # Initialize the global (=canonical) source id counter, which is 1-indexed in fmtomo.
    counter = 1
    # Initialize the list of arrivals dataframes that are to be read from different locations.
    dfs = []
    # Iterate through the list of arrival file paths.
    for f in fs:
        # Load the arrivals data.
        df_tmp = pd.read_csv(f,sep=r"\s+",names=[0,1,2,3,4,5,6])
        # Group the arrivals data by source id (in column 1).
        dfs_tmp = [df for _,df in df_tmp.groupby(1)]
        # Iterate through the source-grouped data.
        for i,ev_id in enumerate(dfs_tmp):
            # Ensure the source id (which might be a normalized source id) is matched to the global (canonical) source id.
            dfs_tmp[i][1] = counter
            # Increment the global source id to account for this source being handled.
            counter += 1
        # Store the source-grouped data after their source ids are converted from core-independent (local/normalized) to core-dependent (global/canonical).
        dfs.extend(dfs_tmp)
    # Combine all source-grouped arrivals data together into one dataframe.
    df = pd.concat(dfs)
    # Reset ray index column.
    df[0] = np.arange(1,len(df)+1)
    # Save the combined data.
    df.to_csv(out,header=None,index=None,sep="\t")
    return

def combine_ray_sep_data(out,ray_sep_datafiles):
    ''' Combine data in multiple ordered ray separated datafiles (e.g. `frechet.dat` or `rays.dat`) and save to disk. These files have the general format of ray-specific data separated by headers (rows of data) containing ray information (event id and receiver ordering).

    out               | <str>          | path to the file to store the combined output in.
    ray_sep_datafiles | <list> [<str>] | ordered list of paths to the ray separated datafiles whose contents are to be combined.
    '''
    # Initialize ray indexer.
    ray_idx = 1
    # Initialize the global (=canonical) source id counter, which is 1-indexed in fmtomo but set to zero here as increment of this counter happens prior to the first instance of it being used to modify a ray data block.
    counter = 0
    # Initialize the list of lines that containing the combined per-ray data.
    all_lines = []
    # Initialize counter of the number of events that have been considered.
    n_evs_prev = 0
    # Iterate through the list of ray separated datafile paths.
    for rs_f in ray_sep_datafiles:
        # Load ray separated data in line format.
        with open(rs_f) as infile:
            data = [l for l in infile.read().split("\n") if l.strip()]
        # Initialize lists to hold the lines containing ray (source-receiver) data (the "header" of a data block) and their corresponding line indices.
        lines = []
        idxs = []
        # Iterate through the data lines.
        for i,l in enumerate(data):
            # Convert string data rows into column-separated data lines.
            l_data = [x for x in l.split(" ") if x]
            # Check whether the active line is a header line (more than 3 data columns) or ray data line.
            if len(l_data) > 3:
                # If the active line is a header line, store the line and line index.
                lines.append(l_data)
                idxs.append(i)
        # Store the final line index to bracket all ray data lines.
        idxs.append(len(data))
        # Initialize list to hold the considerd normalized ids of events.
        unique_evs = []
        # Iterate through all of the header lines.
        for i,l_data in enumerate(lines):
            # Extract the normalized event id from the header line.
            ev = l_data[1]
            # Check whether this is the first header line with this normalize event id in the active ray data.
            if ev not in unique_evs:
                # If so, add this normalized event id to the list of considered, and increment the global event counter.
                unique_evs.append(ev)
                counter += 1
            # Ensure the ray index is as expected.
            l_data[0] = str(ray_idx)
            # Ensure the event id in the header line corresponds to the event's global event id, and update the active ray data.
            l_data[1] = str(counter)
            lines[i] = l_data
            ray_idx += 1
        # Convert the column-separated data lines back into string rows.
        for idx,line in zip(idxs,lines):
            data[idx] = "\t".join(line)
        if "frechet" in out:
            # Check if relocation was turned on - if so, need to correct the last 4 frechet indices.
            if check_source_inversion():
                # Modify the final 4 frechet lines per event.
                for idx in idxs[1:]:
                    for subtract in range(1,5):
                        bad_frech = [x for x in data[idx-subtract].replace("\t"," ").split(" ") if x.strip()]
                        bad_frech[0] = str(int(bad_frech[0]) + 4 * n_evs_prev)
                        data[idx-subtract] = "\t".join(bad_frech)
        # Update the number of events that have been considered.
        n_evs_prev += len(unique_evs)
        # Store these string rows into the combined ray data.
        all_lines.extend(data)
    # Save the combined data.
    with open(out,"w") as outfile:
        outfile.write("\n".join(all_lines))
    return

def combine_arrtimes(arrtimes_fs):
    ev_counter = 0
    all_lines = []
    for i,arrtimes_f in enumerate(arrtimes_fs):
        with open(arrtimes_f) as infile:
            data = [l for l in infile.read().split("\n") if l.strip()]
        # Add the overall header just once.
        if i == 0:
            all_lines.extend(data[:4])
        data = data[4:]
        for l in data:
            possible_header = [x for x in l.split(" ") if x.strip()]
            if len(possible_header) > 1:
                # Is header
                ev_counter += 1
                possible_header[0] = str(ev_counter)
                all_lines.append("           " + "           ".join(possible_header))
            else:
                all_lines.append(l)
    all_lines[3] = "          " + str(ev_counter)
    with open("arrtimes.dat","w") as outfile:
        outfile.write("\n".join(all_lines))
    return

def get_n_sources(wd="./"):
    with open(os.path.join(wd,"sources.in")) as infile:
        n_sources = int(infile.read().split("\n")[0].strip())
    return n_sources

def generate_gridsave(wd):
    n_sources = get_n_sources(wd)
    gridsave = ""
    for i in range(1,n_sources+1):
        gridsave += "%u 1\n1\n1\n" % i
    with open(os.path.join(wd,"gridsave.in"),"w") as outfile:
        outfile.write(gridsave)
    return

def execute(working_dir):
    ''' Handle fm3d execution within some working directory that's different to the current working directory (which must contain all of the files necessary for fm3d execution, minus `sources.in` and `receivers.in`).

    working_dir | <str> | path to the working dir where fm3d will be executed. `sources.in` and `receivers.in` must be present in this directory.
    '''
    # Copy input and auxiliary files necessary for fm3d execution from the current working dir.
    files = ["frechgen.in","interfaces.in","interfacesref.in","propgrid.in","vgrids.in","vgridsref.in","mode_set.in","ak135.hed","ak135.tbl","invert3d.in"]
    for f in files:
        try:
            os.symlink(os.path.join(os.getcwd(),f),os.path.join(working_dir,f))
        except:
            print("Failed to find file",f)
            if check_source_inversion() and f=="invert3d.in":
                raise FileNotFoundError("With source relocation turned on, invert3d.in must be present in this directory!!!!!!!")
    shutil.copy("frechet.in",working_dir)
    # If source inversion is turned on, then frechgen needs to be rerun in each core's subdirectory.
    # if check_source_inversion():
    #     shutil.copy(os.path.join(os.getcwd(),"invert3d.in"),os.path.join(working_dir,"invert3d.in"))
    # Ensure frechet.in is specific to the core's subset of events.
    fmtomo("frechgen",working_dir)
    generate_gridsave(working_dir)
    # Execute fm3d.
    fmtomo("fm3d",working_dir)
    return

def parallel(f,cores,args):
    ''' Execute a single-input function across a list of inputs on multiple cores.

    f     | Function (T1->T2) | single-input function.
    cores | <int>             | number of cores to execute over.
    args  | <list> [T1]       | list of arguments to be inputted into the function under different calls.

    Returns: <list> [T2] | list of outputs from the function.
    '''
    with Pool(cores) as p:
        out = p.map(f,args)
    return out

if __name__=="__main__":
    # Show the number of requested cores.
    print("Running on",cores,"cores")
    # Declare the tmp dir into which source-split subfolders will be saved.
    active_dir = ".tmp"
    # Ensure this tmp dir exists.
    if not os.path.exists(active_dir):
        os.mkdir(active_dir)
    # Clean this tmp dir in case there's anything inside it.
    for f in os.listdir(active_dir):
        shutil.rmtree(os.path.join(active_dir,f))
    # Split the sources across the desired number of cores.
    pick_wds,cores = split_sources(cores,tmp=active_dir)
    # Execute fm3d on all of the sources sublists.
    parallel(execute,cores,pick_wds)
    print("Finished")
    # Join up the outputs from fm3d execution on all sources sublists.
    arrival_fs = [os.path.join(active_dir,str(i),"arrivals.dat") for i in range(cores)]
    frechet_fs = [os.path.join(active_dir,str(i),"frechet.dat") for i in range(cores)]
    ray_fs = [os.path.join(active_dir,str(i),"rays.dat") for i in range(cores)]
    arrtimes_fs = [os.path.join(active_dir,str(i),"arrtimes.dat") for i in range(cores)]
    combine_arrivals("arrivals.dat",arrival_fs)
    combine_ray_sep_data("frechet.dat",frechet_fs)
    if os.path.exists(ray_fs[0]):
        combine_ray_sep_data("rays.dat",ray_fs)
    if os.path.exists(arrtimes_fs[0]):
        combine_arrtimes(arrtimes_fs)
    # Remove the tmp dir.
    shutil.rmtree(active_dir)
