from abc import ABC, abstractmethod
import os
import shutil


from ifcb import DataDirectory

# storage (initially, just file storage)

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


class OutputDirectory(WritableBinStore):
    def __init__(self, root_path, layout='flat'):
        self.root_path = root_path
        self.layout = layout
    
    def write_bin(self, raw_bin, skip_existing=True):
        if self.layout == 'flat':
            source_basepath = raw_bin.basepath # currently only FilesetBins have this property
            dest_basepath = os.path.join(self.root_path, raw_bin.pid)
            for ext in ['hdr', 'adc', 'roi']:
                source_path = f'{source_basepath}.{ext}'
                dest_path = f'{dest_basepath}.{ext}'
                if skip_existing and os.path.exists(dest_path):
                    continue
                shutil.copy2(source_path, dest_path) # FIXME handle errors
        else:
            raise KeyError(f'no such layout: {self.layout}')


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
        

class BinQuerySet(object):
    def __init__(self, qs):
        self.qs = qs
        self.store = None
        
    def with_data(self, store: BinStore):
        self.store = store
        return self
        
    def _copy(self, destination: WritableBinStore, skip_missing=True, skip_existing=True):
        for b in self.qs:
            try:
                raw_bin = self.store.get_bin(b.pid)
                destination.write_bin(raw_bin, skip_existing=skip_existing)
            except: # FIXME create a "bin not found" exception class
                if not skip_missing:
                    raise
            
    def copy(self, path, skip_missing=True, skip_existing=True):
        output_directory = OutputDirectory(path)
        self._copy(output_directory, skip_missing=skip_missing, skip_existing=skip_existing)
        return self