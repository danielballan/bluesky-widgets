import collections
import event_model
from .event import EmitterGroup, Event


class DocumentCache(event_model.SingleRunDocumentRouter):
    def __init__(self):
        self.descriptors = {}
        self.resources = {}
        self.event_pages = collections.defaultdict(list)
        self.datum_pages_by_resource = collections.defaultdict(list)
        self.resource_uid_by_datum_id = {}
        self.start_doc = None
        self.stop_doc = None
        self.events = EmitterGroup(new_stream=Event, new_data=Event, completed=Event)
        self._stream_names = set()

    def start(self, doc):
        self.start_doc = doc

    def stop(self, doc):
        self.stop_doc = doc
        self.events.completed()

    def event_page(self, doc):
        self.event_pages[doc["descriptor"]].append(doc)
        self.events.new_data()

    def datum_page(self, doc):
        self.datum_pages_by_resource[doc["resource"]].append(doc)
        for datum_id in doc["datum_id"]:
            self.resource_uid_by_datum_id[datum_id] = doc["resource"]

    def descriptor(self, doc):
        name = doc.get("name")  # Might be missing in old documents
        if name is not None and name not in self._stream_names:
            self._stream_names.add(name)
            self.events.new_stream(name)
        self.descriptors[doc["uid"]] = doc

    def resource(self, doc):
        self.resources[doc["uid"]] = doc


class StreamExists(event_model.EventModelRuntimeError):
    ...


class RunBuilder:
    def __init__(self, metadata=None, uid=None, time=None):
        self._cache = DocumentCache()
        self._run_bundle = event_model.compose_run(
            uid=uid, time=time, metadata=metadata
        )
        self._cache.start(self._run_bundle.start_doc)
        # maps stream name to StreamBuilder
        self._streams = {}

    def __getattr__(self, key):
        try:
            return self._streams[key]
        except KeyError:
            raise AttributeError(key)

    def add_stream(
        self,
        name,
        data_keys,
        uid=None,
        time=None,
        object_keys=None,
        configuration=None,
        hints=None,
    ):
        if name in self._streams:
            raise StreamExists(name)
        builder = StreamBuilder(
            cache=self._cache,
            compose_descriptor=self._run_bundle.compose_descriptor,
            name=name,
            data_keys=data_keys,
            time=time,
            object_keys=object_keys,
            configuration=configuration,
            hints=hints,
        )
        self._streams[name] = builder

    def close(self, exit_status="success", reason="", uid=None, time=None):
        doc = self._run_bundle.compose_stop(
            exit_status=exit_status, reason=reason, uid=uid, time=time
        )
        self._cache.stop(doc)
        for stream in self._streams.values():
            stream._close()

    def get_run(self):
        # TODO Maybe return the same instance every time. Maybe not....
        return BlueskyRun(self._cache)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close(exit_status="failed", reason=repr(type(value)))


class StreamBuilder:
    def __init__(
        self,
        *,
        cache,
        compose_descriptor,
        name,
        data_keys,
        time,
        object_keys,
        configuration,
        hints,
    ):
        self._compose_descriptor = compose_descriptor
        self._cache = cache
        self._closed = False
        self._bundle = compose_descriptor(
            name=name,
            data_keys=data_keys,
            time=time,
            object_keys=object_keys,
            configuration=configuration,
            hints=hints,
        )
        self._cache.descriptor(self._bundle.descriptor_doc)

    def update_configuration(self, name, configuration):
        if self._closed:
            raise event_model.EventModelRuntimeError("Run is closed.")
        doc = self._bundle.descriptor_doc
        self._bundle = self._compose_descriptor(
            name=doc["name"],
            data_keys=doc["data_keys"],
            time=doc["time"],
            object_keys=doc["object_keys"],
            configuration=doc["configuration"],
            hints=doc["hints"],
        )
        self._cache.descriptor(self._bundle.descriptor_doc)

    def add_data(self, data):
        if self._closed:
            raise event_model.EventModelRuntimeError("Run is closed.")

    def _close(self):
        self._closed = True


class BlueskyRun:
    """
    Push-based BlueskyRun
    """

    def __init__(self, document_cache, get_filler):
        self._document_cache = document_cache
        self._get_filler = get_filler

    @property
    def events(self):
        return self._document_cache.events


class BlueskyEventStream:
    def __init__(self, stream_name, document_cache, get_filler):
        ...

    def to_dask(self):

        document_cache = self._document_cache

        def get_event_pages(descriptor_uid, skip=0, limit=None):
            if skip != 0 and limit is not None:
                raise NotImplementedError
            return document_cache.event_pages[descriptor_uid]

        # def get_event_count(descriptor_uid):
        #     return sum(len(page['seq_num'])
        #                for page in (document_cache.event_pages[descriptor_uid]))

        def get_resource(uid):
            return document_cache.resources[uid]

        # def get_resources():
        #     return list(document_cache.resources.values())

        # def lookup_resource_for_datum(datum_id):
        #     return document_cache.resource_uid_by_datum_id[datum_id]

        def get_datum_pages(resource_uid, skip=0, limit=None):
            if skip != 0 and limit is not None:
                raise NotImplementedError
            return document_cache.datum_pages_by_resource[resource_uid]

        filler = get_filler(coerce="delayed")

        ds = _documents_to_xarray(
            start_doc=document_cache.start_doc,
            stop_doc=document_cache.stop_doc,
            descriptor_docs=list(document_cache.descriptors.values()),
            get_event_pages=get_event_pages,
            filler=filler,
            get_resource=get_resource,
            lookup_resource_for_datum=lookup_resource_for_datum,
            get_datum_pages=get_datum_pages,
        )
        return ds

    def read(self):
        return self.to_dask().load()


def _documents_to_xarray(
    *,
    start_doc,
    stop_doc,
    descriptor_docs,
    get_event_pages,
    filler,
    get_resource,
    lookup_resource_for_datum,
    get_datum_pages,
    include=None,
    exclude=None,
):
    """
    Represent the data in one Event stream as an xarray.

    Parameters
    ----------
    start_doc: dict
        RunStart Document
    stop_doc : dict
        RunStop Document
    descriptor_docs : list
        EventDescriptor Documents
    filler : event_model.Filler
    get_resource : callable
        Expected signature ``get_resource(resource_uid) -> Resource``
    lookup_resource_for_datum : callable
        Expected signature ``lookup_resource_for_datum(datum_id) -> resource_uid``
    get_datum_pages : callable
        Expected signature ``get_datum_pages(resource_uid) -> generator``
        where ``generator`` yields datum_page documents
    get_event_pages : callable
        Expected signature ``get_event_pages(descriptor_uid) -> generator``
        where ``generator`` yields event_page documents
    include : list, optional
        Fields ('data keys') to include. By default all are included. This
        parameter is mutually exclusive with ``exclude``.
    exclude : list, optional
        Fields ('data keys') to exclude. By default none are excluded. This
        parameter is mutually exclusive with ``include``.

    Returns
    -------
    dataset : xarray.Dataset
    """
    if include is None:
        include = []
    if exclude is None:
        exclude = []
    if include and exclude:
        raise ValueError(
            "The parameters `include` and `exclude` are mutually exclusive."
        )
    # Data keys must not change within one stream, so we can safely sample
    # just the first Event Descriptor.
    if descriptor_docs:
        data_keys = descriptor_docs[0]["data_keys"]
        if include:
            keys = list(set(data_keys) & set(include))
        elif exclude:
            keys = list(set(data_keys) - set(exclude))
        else:
            keys = list(data_keys)

    # Collect a Dataset for each descriptor. Merge at the end.
    datasets = []
    dim_counter = itertools.count()
    event_dim_labels = {}
    config_dim_labels = {}
    for descriptor in descriptor_docs:
        events = list(_flatten_event_page_gen(get_event_pages(descriptor["uid"])))
        if not events:
            continue
        if any(data_keys[key].get("external") for key in keys):
            filler("descriptor", descriptor)
            filled_events = []
            for event in events:
                filled_event = _fill(
                    filler,
                    event,
                    lookup_resource_for_datum,
                    get_resource,
                    get_datum_pages,
                )
                filled_events.append(filled_event)
        else:
            filled_events = events
        times = [ev["time"] for ev in events]
        seq_nums = [ev["seq_num"] for ev in events]
        uids = [ev["uid"] for ev in events]
        data_table = _transpose(filled_events, keys, "data")
        # external_keys = [k for k in data_keys if 'external' in data_keys[k]]

        # Collect a DataArray for each field in Event, each field in
        # configuration, and 'seq_num'. The Event 'time' will be the
        # default coordinate.
        data_arrays = {}

        # Make DataArrays for Event data.
        for key in keys:
            field_metadata = data_keys[key]
            # if the EventDescriptor doesn't provide names for the
            # dimensions (it's optional) use the same default dimension
            # names that xarray would.
            try:
                dims = tuple(field_metadata["dims"])
            except KeyError:
                ndim = len(field_metadata["shape"])
                # Reuse dim labels.
                try:
                    dims = event_dim_labels[key]
                except KeyError:
                    dims = tuple(f"dim_{next(dim_counter)}" for _ in range(ndim))
                    event_dim_labels[key] = dims
            data_arrays[key] = xarray.DataArray(
                data=data_table[key],
                dims=("time",) + dims,
                coords={"time": times},
                name=key,
            )

        # Make DataArrays for configuration data.
        for object_name, config in descriptor.get("configuration", {}).items():
            data_keys = config["data_keys"]
            # For configuration, label the dimension specially to
            # avoid key collisions.
            scoped_data_keys = {key: f"{object_name}:{key}" for key in data_keys}
            if include:
                keys = {k: v for k, v in scoped_data_keys.items() if v in include}
            elif exclude:
                keys = {k: v for k, v in scoped_data_keys.items() if v not in include}
            else:
                keys = scoped_data_keys
            for key, scoped_key in keys.items():
                field_metadata = data_keys[key]
                ndim = len(field_metadata["shape"])
                # if the EventDescriptor doesn't provide names for the
                # dimensions (it's optional) use the same default dimension
                # names that xarray would.
                try:
                    dims = tuple(field_metadata["dims"])
                except KeyError:
                    try:
                        dims = config_dim_labels[key]
                    except KeyError:
                        dims = tuple(f"dim_{next(dim_counter)}" for _ in range(ndim))
                        config_dim_labels[key] = dims
                data_arrays[scoped_key] = xarray.DataArray(
                    # TODO Once we know we have one Event Descriptor
                    # per stream we can be more efficient about this.
                    data=numpy.tile(
                        config["data"][key], (len(times),) + ndim * (1,) or 1
                    ),
                    dims=("time",) + dims,
                    coords={"time": times},
                    name=key,
                )

        # Finally, make DataArrays for 'seq_num' and 'uid'.
        data_arrays["seq_num"] = xarray.DataArray(
            data=seq_nums, dims=("time",), coords={"time": times}, name="seq_num"
        )
        data_arrays["uid"] = xarray.DataArray(
            data=uids, dims=("time",), coords={"time": times}, name="uid"
        )

        datasets.append(xarray.Dataset(data_vars=data_arrays))
    # Merge Datasets from all Event Descriptors into one representing the
    # whole stream. (In the future we may simplify to one Event Descriptor
    # per stream, but as of this writing we must account for the
    # possibility of multiple.)
    return xarray.merge(datasets)
