from .models import Bin

from .backend import IterableBinStore, FilesetBinStore, BinQuerySet


def data_directory(path: str):
    return FilesetBinStore(path)


def import_bins(store: IterableBinStore, update=True, skip_existing=True):
    for b in store:
        pid = b.pid
        metadata = {
            'timestamp': b.timestamp,
            'sample_time': b.timestamp, # default. modify using upload_metadata
            'instrument': b.instrument           
        }
        if update:
            Bin.objects.update_or_create(pid=pid, defaults=metadata)
        else: # new only
            try:
                Bin.objects.create(pid=pid, **metadata)
            except:
                raise # FIXME for object already exists, do not raise if skip_existing is True
            
            
def select_bins(instrument=None, start_time=None, end_time=None):
    # TODO will eventually have a lot more parameters
    qs = Bin.objects
    
    if instrument is not None:
        qs = qs.filter(instrument=instrument)
    
    if start_time is not None:
        qs = qs.filter(sample_time__gte=start_time)
        
    if end_time is not None:
        qs = qs.filter(sample_time__lte=end_time)

    return BinQuerySet(qs)

