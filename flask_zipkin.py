#
from flask import g
from flask import request
from flask import _app_ctx_stack
from flask import current_app

import socket
import requests
from py_zipkin import zipkin


__version_info__ = ('0', '0', '2')
__version__ = '.'.join(__version_info__)
__author__ = 'killpanda'
__license__ = 'BSD'
__copyright__ = '(c) 2016 by killpanda'
__all__ = ['Zipkin']


class Zipkin(object):

    PREAMBLE = str.encode('\x0c\x00\x00\x00\x01')

    def _gen_random_id(self):
        return zipkin.generate_random_64bit_string()

    def __init__(self, app=None, sample_rate=100):
        self._exempt_views = set()
        self._sample_rate = sample_rate
        self._transport_handler = None
        self._transport_exception_handler = None
        self.zipkin_dsn = None
        self.service_name = 'unknown'
        self.machine_name = socket.getfqdn()

        if app is not None:
            self.init_app(app)

    def default_exception_handler(self, ex):
        pass

    def default_handler(self, encoded_span):
        try:
            body = self.PREAMBLE + encoded_span
            return requests.post(
                self.zipkin_dsn,
                data=body,
                headers={'Content-Type': 'application/x-thrift'},
                timeout=1,
            )
        except Exception as e:
            if self._transport_exception_handler:
                self._transport_exception_handler(e)
            else:
                self.default_exception_handler(e)

    def transport_handler(self, callback):
        self._transport_handler = callback
        return callback

    def transport_exception_handler(self, callback):
        self._transport_exception_handler = callback
        return callback

    def init_app(self, app):
        # type: (flask.Flask) -> Zipkin
        self.service_name = app.name
        self.zipkin_dsn = app.config.get('ZIPKIN_DSN')

        if not hasattr(app, 'extensions'):
            app.extensions = {}  # pragma: no cover
        app.extensions['zipkin'] = self

        app.before_request(self._before_request)
        app.after_request(self._after_request)

        self._disable = app.config.get(
            'ZIPKIN_DISABLE', app.config.get('TESTING', False))
        return self

    def _should_use_token(self, view_func):
        return (view_func not in self._exempt_views)

    def _before_request(self):
        if self._disable:
            return

        _app_ctx_stack.top._view_func = \
            current_app.view_functions.get(request.endpoint)

        if not self._should_use_token(_app_ctx_stack.top._view_func):
            return
        headers = request.headers
        trace_id = headers.get('X-B3-TraceId') or self._gen_random_id()
        parent_span_id = headers.get('X-B3-Parentspanid')
        is_sampled = str(headers.get('X-B3-Sampled') or '0') == '1'
        flags = headers.get('X-B3-Flags')

        zipkin_attrs = zipkin.ZipkinAttrs(
            trace_id=trace_id,
            span_id=self._gen_random_id(),
            parent_span_id=parent_span_id,
            flags=flags,
            is_sampled=is_sampled,
        )

        handler = self._transport_handler or self.default_handler

        span = zipkin.zipkin_server_span(
            service_name='wsgi.flask:' + self.service_name,
            span_name='{}:{}'.format(request.method.upper(), request.endpoint),
            transport_handler=handler,
            sample_rate=self._sample_rate,
            zipkin_attrs=zipkin_attrs
        )
        g._zipkin_span = span
        g._zipkin_span.start()

        self.annotate_before_request(span)

    def annotate_before_request(self, span):
        # type: (py_zipkin.zipkin.zipkin_client_span) -> None

        # Keys found https://github.com/openzipkin/zipkin/blob/master/zipkin/src/main/java/zipkin/TraceKeys.java
        span.update_binary_annotations_for_root_span({
            'wsgi.name': self.service_name,
            'wsgi.framework': 'flask',
            'wsgi.machine': self.machine_name,
            'http.method': request.method,
            'http.path': request.path,
            'http.host': request.host,
        })

    def exempt(self, view):
        view_location = '{0}.{1}'.format(view.__module__, view.__name__)
        self._exempt_views.add(view_location)
        return view

    def _after_request(self, response):
        if self._disable:
            return response
        if not hasattr(g, '_zipkin_span'):
            return response

        span = g._zipkin_span
        self.annotate_after_request(span, response)
        span.stop()
        return response

    def annotate_after_request(self, span, response):
        # type: (py_zipkin.zipkin.zipkin_client_span, flask.Response) -> None
        span.update_binary_annotations_for_root_span({
            'http.status_code': response.status_code,
        })

    def create_http_headers_for_new_span(self):
        if self._disable:
            return dict()
        return zipkin.create_http_headers_for_new_span()

    def logging(self, **kwargs):
        if g._zipkin_span and g._zipkin_span.zipkin_attrs.is_sampled and g._zipkin_span.logging_context:
            g._zipkin_span.logging_context.binary_annotations_dict.update(
                kwargs)
