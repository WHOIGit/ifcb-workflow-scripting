from abc import ABC, abstractmethod
import os
import shutil
import re

from .models import Bin

from ifcb import DataDirectory, Pid


class BinStore(ABC):
    pass


class ReadableBinStore(BinStore):
    @abstractmethod
    def get_bin(self, pid):
        "retrieve a specific bin"
        pass

 
class WritableBinStore(BinStore):
    @abstractmethod
    def write_bin(self, raw_bin):
        pass
    
    
class IterableBinStore(ReadableBinStore):
    @abstractmethod
    def bins(self):
        "return an iterator over all bins"
        pass
    
    def __iter__(self):
        return self.bins()


class FilesetBinStore(IterableBinStore):
    def __init__(self, path):
        self.path = path
        self.dd = DataDirectory(path)
        
    def get_bin(self, pid):
        return FilesetBin(self.dd[pid])
    
    def bins(self):
        for b in self.dd:
            yield FilesetBin(b)
        
    def import_bins(self, update=True, skip_existing=True):
        bin_pids = []
        
        for b in self:
            pid = b.pid
            metadata = {
                'timestamp': b.timestamp,
                'sample_time': b.timestamp, # default. modify using upload_metadata
                'instrument': b.instrument           
            }
            if update:
                Bin.objects.update_or_create(pid=pid, defaults=metadata)
                bin_pids.append(pid)
            else: # new only
                try:
                    Bin.objects.create(pid=pid, **metadata)
                    bin_pids.append(pid)
                except: # FIXME catch relevant exception
                    raise # FIXME for object already exists, do not raise if skip_existing is True
                
        qs = Bin.objects.filter(pid__in=bin_pids) # does this scale?
        
        return BinQuerySet(qs).with_data(self)


def compute_basepath(pid, layout='flat'):
    if layout == 'flat':
        return pid
    elif layout == 'day':
        parsed_pid = Pid(pid)
        return os.path.join(parsed_pid.yearday, pid)
    elif layout == 'yearday':
        parsed_pid = Pid(pid)
        return os.path.join(parsed_pid.year, parsed_pid.yearday, pid)
    raise KeyError(f'no such layout: {layout}')

class OutputDirectory(WritableBinStore):
    def __init__(self, root_path, layout='flat'):
        self.root_path = root_path
        self.layout = layout
    
    def write_bin(self, raw_bin, skip_existing=True):
        dest_basepath = compute_basepath(raw_bin.pid, self.layout)
        source_basepath = raw_bin.basepath # currently only FilesetBins have this property
        dest_basepath = os.path.join(self.root_path, dest_basepath)
        for ext in ['hdr', 'adc', 'roi']:
            source_path = f'{source_basepath}.{ext}'
            dest_path = f'{dest_basepath}.{ext}'
            if skip_existing and os.path.exists(dest_path):
                continue
            os.makedirs(os.path.dirname(dest_path), exist_ok=True) # FIXME cache existence info for peformance
            shutil.copy2(source_path, dest_path) # FIXME handle errors


class RawBin(ABC):
    @property
    @abstractmethod
    def pid(self):
        pass

    @property
    @abstractmethod
    def timestamp(self):
        pass
        
    @property
    @abstractmethod
    def instrument(self):
        pass
        

class FilesetBin(RawBin):
    def __init__(self, fileset_bin):
        self.fileset_bin = fileset_bin
    
    @property
    def pid(self):
        return self.fileset_bin.pid.bin_lid
    
    @property
    def timestamp(self):
        return self.fileset_bin.pid.timestamp
    
    @property
    def instrument(self):
        return self.fileset_bin.pid.instrument
    
    @property
    def basepath(self):
        return self.fileset_bin.fileset.basepath


def normalize_tag_name(tag_name):
    normalized = re.sub(r'[^_a-zA-Z0-9]','_',tag_name.lower().strip())
    return normalized


class BinQuerySet(object):
    def __init__(self, qs):
        self.qs = qs
        self.store = None
        
    def with_data(self, store: BinStore):
        self.store = store
        return self
    
    def filter(self, instrument=None, start_time=None, end_time=None,
               min_depth=None, max_depth=None, sample_type=None,
               cruise=None, tag=None):
        # TODO will eventually have a lot more parameters
        qs = self.qs
        
        if instrument is not None:
            qs = qs.filter(instrument=instrument)
        
        if start_time is not None:
            qs = qs.filter(sample_time__gte=start_time)
            
        if end_time is not None:
            qs = qs.filter(sample_time__lte=end_time)
        
        if min_depth is not None:
            qs = qs.filter(depth__gte=min_depth)
            
        if max_depth is not None:
            qs = qs.filter(depth__lte=max_depth)
            
        if sample_type is not None:
            qs = qs.filter(sample_type=sample_type)
            
        if cruise is not None:
            qs = qs.filter(cruise=cruise)
        
        if tag is not None:
            normalized_tag_name = normalize_tag_name(tag)
            qs = qs.filter(tags__name=normalized_tag_name)

        self.qs = qs 
        
        return self
    
    def export_list(self, output_path):
        with open(output_path,'w') as fout:
            for b in self.qs:
                print(b.pid, file=fout)
                
        return self

    def _copy(self, destination: WritableBinStore, skip_missing=True, skip_existing=True):
        for b in self.qs:
            try:
                raw_bin = self.store.get_bin(b.pid)
                destination.write_bin(raw_bin, skip_existing=skip_existing)
            except: # FIXME create a "bin not found" exception class
                if not skip_missing:
                    raise
            
    def copy(self, path, layout='flat', skip_missing=True, skip_existing=True):
        output_directory = OutputDirectory(path, layout=layout)
        self._copy(output_directory, skip_missing=skip_missing, skip_existing=skip_existing)
        return self
    
    def tag(self, tag_name):
        tag_name = normalize_tag_name(tag_name)
        
        for b in self.qs:
            b.add_tag(tag_name)
            
        return self
            
    def remove_tag(self, tag_name):
        tag_name = normalize_tag_name(tag_name)
        
        for b in self.qs:
            b.remove_tag(tag_name)
            
        return self
    
    def count(self):
        return self.qs.count()
    
    def set(self, **kw):
        settable_fields = ['lat','lon','depth','sample_type','cruise']
        
        if not all([k in settable_fields for k in kw.keys()]):
            raise KeyError('unrecognized attribute') # FIXME say which one
        
        self.qs.update(**kw)
        
        return self
