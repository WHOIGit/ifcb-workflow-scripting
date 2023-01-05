from .models import Bin

from .backend import FilesetBinStore, BinQuerySet


def data_directory(path: str):
    return FilesetBinStore(path)
            
         
def select_bins(**kw):
    qs = BinQuerySet(Bin.objects.all())
    
    return qs.filter(**kw)

