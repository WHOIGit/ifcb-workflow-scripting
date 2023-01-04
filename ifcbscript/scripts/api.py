from .models import Bin

from .backend import IterableBinStore, FilesetBinStore, BinQuerySet


def data_directory(path: str):
    return FilesetBinStore(path)


def import_bins(store: IterableBinStore, update=True, skip_existing=True):
    bin_pids = []
    
    for b in store:
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
            
    qs = Bin.objects.filter(pid__in=bin_pids)
    
    return BinQuerySet(qs).with_data(store)
            
         
def select_bins(**kw):
    qs = BinQuerySet(Bin.objects.all())
    
    return qs.filter(**kw)

