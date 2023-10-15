import os
import os.path
import pickle
import warnings

from joblib import register_store_backend, numpy_pickle
from joblib._store_backends import FileSystemStoreBackend, CacheWarning


class FileSystemStoreBackendNoNumpy(FileSystemStoreBackend):
    NAME = "no_numpy"

    def load_item(self, path, verbose=1, msg=None):
        """Load an item from the store given its path as a list of
           strings."""
        full_path = os.path.join(self.location, *path)

        if verbose > 1:
            if verbose < 10:
                print('{0}...'.format(msg))
            else:
                print('{0} from {1}'.format(msg, full_path))

        mmap_mode = (None if not hasattr(self, 'mmap_mode')
                     else self.mmap_mode)

        filename = os.path.join(full_path, 'output.pkl')
        if not self._item_exists(filename):
            raise KeyError("Non-existing item (may have been "
                           "cleared).\nFile %s does not exist" % filename)

        # file-like object cannot be used when mmap_mode is set
        if mmap_mode is None:
            with self._open_item(filename, "rb") as f:
                item = pickle.load(f)
        else:
            item = numpy_pickle.load(filename, mmap_mode=mmap_mode)
        return item

    def dump_item(self, path, item, verbose=1):
        """Dump an item in the store at the path given as a list of
           strings."""
        try:
            item_path = os.path.join(self.location, *path)
            if not self._item_exists(item_path):
                self.create_location(item_path)
            filename = os.path.join(item_path, 'output.pkl')
            if verbose > 10:
                print('Persisting in %s' % item_path)

            def write_func(to_write, dest_filename):
                mmap_mode = (None if not hasattr(self, 'mmap_mode')
                             else self.mmap_mode)
                with self._open_item(dest_filename, "wb") as f:
                    try:
                        if mmap_mode is None:
                            pickle.dump(to_write, f)
                        else:
                            numpy_pickle.dump(to_write, f, compress=self.compress)
                    except pickle.PicklingError as e:
                        # TODO(1.5) turn into error
                        warnings.warn(
                            "Unable to cache to disk: failed to pickle "
                            "output. In version 1.5 this will raise an "
                            f"exception. Exception: {e}.",
                            FutureWarning
                        )

            self._concurrency_safe_write(item, filename, write_func)
        except Exception as e:  # noqa: E722
            warnings.warn(
                "Unable to cache to disk. Possibly a race condition in the "
                f"creation of the directory. Exception: {e}.",
                CacheWarning
            )


register_store_backend(FileSystemStoreBackendNoNumpy.NAME, FileSystemStoreBackendNoNumpy)
