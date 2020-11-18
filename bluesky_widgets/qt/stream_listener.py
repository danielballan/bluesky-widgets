import pickle

from bluesky.run_engine import Dispatcher, DocumentNames
import zmq

from ..qt.threading import create_worker
from qtpy.QtCore import QTimer, QObject


LOADING_LATENCY = 0.01


class RemoteDispatcher(QObject):
    """
    Dispatch documents received over the network from a 0MQ proxy.

    Parameters
    ----------
    address : tuple
        Address of a running 0MQ proxy, given either as a string like
        ``'127.0.0.1:5567'`` or as a tuple like ``('127.0.0.1', 5567)``
    prefix : bytes, optional
        User-defined bytestring used to distinguish between multiple
        Publishers. If set, messages without this prefix will be ignored.
        If unset, no mesages will be ignored.
    deserializer: function, optional
        optional function to deserialize data. Default is pickle.loads

    Examples
    --------

    Print all documents generated by remote RunEngines.

    >>> d = RemoteDispatcher(('localhost', 5568))
    >>> d.subscribe(print)
    >>> d.start()  # runs until interrupted
    """

    def __init__(self, address, *, prefix=b"", deserializer=pickle.loads, parent=None):
        super().__init__(parent)
        if isinstance(prefix, str):
            raise ValueError("prefix must be bytes, not string")
        if b" " in prefix:
            raise ValueError("prefix {!r} may not contain b' '".format(prefix))
        self._prefix = prefix
        if isinstance(address, str):
            address = address.split(":", maxsplit=1)
        self._deserializer = deserializer
        self.address = (address[0], int(address[1]))

        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.SUB)
        url = "tcp://%s:%d" % self.address
        self._socket.connect(url)
        self._socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self._task = None
        self.closed = False
        self._timer = QTimer(self)
        self._dispatcher = Dispatcher()
        self.subscribe = self._dispatcher.subscribe
        self._waiting_for_start = True

    def _receive_data(self):
        our_prefix = self._prefix  # local var to save an attribute lookup
        message = self._socket.recv()
        prefix, name, doc = message.split(b" ", 2)
        name = name.decode()
        if (not our_prefix) or prefix == our_prefix:
            if self._waiting_for_start:
                if name == "start":
                    self._waiting_for_start = False
                else:
                    # We subscribed midstream and are seeing documents for
                    # which we do not have the full run. Wait for a 'start'
                    # doc.
                    return
            doc = self._deserializer(doc)
        return name, doc

    def start(self):
        if self.closed:
            raise RuntimeError(
                "This RemoteDispatcher has already been "
                "started and interrupted. Create a fresh "
                "instance with {}".format(repr(self))
            )
        self._work_loop()

    def _work_loop(self):
        worker = create_worker(
            self._receive_data,
        )
        # Schedule this method to be run again after a brief wait.
        worker.finished.connect(
            lambda: self._timer.singleShot(LOADING_LATENCY, self._work_loop)
        )
        worker.returned.connect(self._process_result)
        worker.start()

    def _process_result(self, result):
        if result is None:
            return
        name, doc = result
        self._dispatcher.process(DocumentNames[name], doc)

    def stop(self):
        self.closed = True
