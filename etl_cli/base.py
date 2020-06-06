import os
import sys
import logging
from argparse import ArgumentParser
from pprint import pprint as pp
import time
import math

from slovar import slovar
from slovar.convert import qs2dict
from slovar.strings import split_strip

from prf.request import PRFRequest

log = logging
log.basicConfig(level=logging.INFO)


class CLIError(Exception):
    pass

def parse_ds(name, **overwrites):
    SEP = '/'
    if not name or isinstance(name, dict):
        return name

    if '%TODAY%' in name:
        name = name.replace('%TODAY%',
                datetime.utcnow().strftime('%Y-%m-%d')
            )

    params = slovar()
    name_parts = name.split(SEP)
    params.backend = name_parts[0]
    params.ns = SEP.join(name_parts[1:-1])
    params.name = name_parts[-1]
    params.update(overwrites)
    return params


class Args(slovar):
    '''This class allows to keep the initial values in the args from overwrites.
    Will make args passed from command line overwrite the "default" values in various scripts.
    Eliminates the need for things like `args.abc = args.abs or '123'`
    '''

    MUTABLES = []

    def __init__(self, arg_dict, mutables=[], _raise=False):
        Args.MUTABLES = mutables
        self._raise = _raise
        super().__init__(arg_dict)

    def __setitem__(self, key, value):
        if key in self and key not in Args.MUTABLES and self[key] is not None:
            msg = 'Trying to change value for `%s` from `%s` to `%s`' % (
                                    key, self[key], value
                            )
            if self._raise:
                raise ValueError(msg)
            else:
                log.warning(msg)
        else:
            super().__setitem__(key, value)

    def copy(self):
        #deep copy return object of the same Args class (vs copy returning slovar)
        #also it "actually" copies the object, which is important for this class (keep immutability for the original)
        return self.deepcopy()

    def to_slovar(self):
        return slovar(self)

class Base(object):

    MUTABLE_OPS = [
        'arg',
        'mongo_index',
        'fail_on_error',
        'info',
        'dev',
        'verbose',
        'silent'
    ]
    MORPHER_MODULE = 'smurfs'

    @classmethod
    def trans_name(cls, path):
        path,_,args = path.partition(':')

        if cls.MORPHER_MODULE not in path:
            path = '%s.%s' % (cls.MORPHER_MODULE, path)

        return path, split_strip(args)

    @classmethod
    def process_transformers(cls, trs, ds_name):
        params = {}
        _trs = []

        ds, call = ds_name.split('.')

        for tr in split_strip(trs):
            _tr, tr_args = cls.trans_name(tr)
            _trs.append(_tr)

        params['%s.transformer.%s' % (ds, call)] = ','.join(_trs)
        return params

    def __init__(self):

        self.add_argument('--etl_api', default='localhost:6544/api',
                                    help='base url of etl service')
        self.add_argument('--std_api', default='localhost:6544/api/standards',
                                    help='standards service api')
        self.add_argument('--drop', help='drop target first', action='store_true')
        self.add_argument('--drop-ns', help='drop target namespace', action='store_true')
        self.add_argument('--dry', help='print out params', action='store_true')
        self.add_argument('--workers', help='nb of workers', type=int)

        self.add_argument('--batch', help='batch_size')
        self.add_argument('--dev', action='store_true', help='dev mode will run request in async mode on server')
        self.add_argument('--silent', action='store_true')
        self.add_argument('--log', help='turn on logs', default='-logs,-set__logs')
        self.add_argument('--pretty', action='store_true', help='pretty logs')

        self.add_argument('-a', '--arg', action='append', default=[],
                                    help= 'extra args can be specified multiple times')

        self.add_argument('--no-count', action='store_true', help='skip doing count')
        self.add_argument('--pk')
        self.add_argument('--info', help='just show info and quit')
        self.add_argument('--msg', help='job comment')
        self.add_argument('--poll-interval', type=float, default=10, help='sleep seconds between repeats')
        self.add_argument('--show-args', help='non-blocking', action='store_true')
        self.add_argument('--cron', help='schedule cron')
        self.add_argument('--fail-on-error', help='fail on error', default=True)

        self.add_argument('--paginate', help='fast pagination')
        self.add_argument('--mongo-index', help='create mongo index', action='append', default=[])
        self.add_argument('--skip-index', help='skip mongo indexing', action='store_true')
        self.add_argument('--verbose', help='verbose logging on server', action='store_true')
        self.add_argument('--profile', help='run cProfiler', action='store_true')
        self.add_argument('--pylog2es', help='enable python logging to elastic', action='store_true')
        self.add_argument('--log_ds', help='elastic ns to save target logs', default='logs')
        self.add_argument('--log-level', help='set log level for server')

        self.add_argument('--ns', help='source ns')

        _parsed_args = self.parser.parse_args()

        self.args = Args(vars(_parsed_args), mutables=self.MUTABLE_OPS)

        self.api = self.get_api(_raise=True)
        self._datasets = slovar()

        self.source_total = None
        self.job_uid = None

    def args2env_var(self, args):
        arg_name = ''
        for arg in args:
            if arg.startswith('--'):
                arg_name = arg[2:].replace('-', '_')
            elif arg.startswith('-'):
                arg_name = arg[1:].replace('-', '_')

        if not arg_name:
            raise ValueError('This should not have happened with args = %s' % str(args))

        return '.'.join([self.__module__,arg_name]).replace('.', '__').upper()

    def add_argument(self, *args, **kw):

        if 'env_var' in kw:
            var_name = kw.pop('env_var')
        else:
            var_name = self.args2env_var(args)

        if var_name and var_name in os.environ:
            kw['default'] = os.environ.get(var_name)
            print('%s=%s' % (var_name, kw['default']))

        if not hasattr(self, 'parser'):
            self.parser = self.get_arg_parser()

        self.parser.add_argument(*args, **kw)

    def pprint(self, item):
        pp(item)

    def required(self, name):
        if self.args.get(name) is None:
            self.parser.error('%s required' % name)

    def ask_input(self, msg, ignore_enter=False):
        if self.args.silent:
            return True

        # if self.args.dry:
        #     input(msg + 'Enter to Continue: ')
        #     return True

        inp = input(msg + 'yes/no/silent: ')

        if inp.startswith('y'):
            return True
        elif inp.startswith('n'):
            return False
        elif inp.startswith('s'):
            self.args.silent=True
            return True
        else:
            if not ignore_enter:
                self.error('Input canceled')
            return False

    def qlsit2query(self, qs_list, prefix=''):
        _pp = slovar()
        for qs in qs_list:
            for kk,vv in qs2dict(qs).items():
                fld_name = '%s%s'%(prefix, kk)
                if kk == '_fields':
                    _pp.add_to_list(fld_name, vv)
                else:
                    _pp[fld_name] = vv

        if '%s_fields' % prefix in _pp:
            fname = '%s_fields' % prefix
            _pp[fname] = ','.join(_pp[fname])

        return _pp

    def get_arg_parser(self, **kw):
        kw.setdefault('description', self.__doc__)
        return getattr(self, 'parser', ArgumentParser(**kw))

    def setup(self, args, params=None):

        params = params or slovar()
        params.args = args

        if args.dry:
            params['target.dry_run'] = 1
            params['target.log_pretty'] = 1
            args.dev = True
            args.batch = 1
            args.setdefault('batch', 1)

        if args.pk:
            params['target.pk'] = args.pk

        if args.pretty:
            params['target.log_pretty'] = 1

        if args.workers:
            params['workers'] = args.workers

        if args.dev:
            params['async'] = 0

        if args.log is not None or args.dev:
            params['target.log_size'] = 10000
            params['target.log_pretty'] = True
            params['target.log_fields'] = args.log
        else:
            params['target.log_fields'] = '-log,-logs,-source'

        params['target.fail_on_error'] = args.asbool('fail_on_error')

        params['batch_size'] = args.batch or 1000

        for each in args.arg:
            name, _, value = each.partition('=')
            params[name] = value

        params['comment'] = args.msg
        params['cron'] = args.cron

        if args.get('contid'):
            params['contid'] = args['contid']

        if args['dry']:
            args['verbose'] = True

        if args['verbose']:
            params['target.verbose_logging'] = 1

        if args['profile']:
            params['profile'] = 1

        if args['pylog2es']:
            params['pylog2es'] = 1

        if args['log_level']:
            params['log_level'] = args['log_level']

        self._datasets = slovar()

        return params.pop_by_values([None]).unflat()

    def parse_ds(self, ds_name, **overwrites):
        return parse_ds(ds_name, **overwrites)

    def setup_ds(self, ds_name, field_name, required=False, **overwrites):
        params = slovar({field_name:self.parse_ds(ds_name, **overwrites)})

        if not params[field_name]:
            if required:
                self.error('%s is required' % field_name)
            else:
                return slovar()

        self._datasets[field_name] = params.extract('%s.*' % field_name)
        return params

    def add_to_datasets(self, name, ds):
        self._datasets[field_name] = params.extract('%s.*' % field_name)

    def ds2uri(self, ds):
        return ds
        if not ds:
            return
        return '{backend}/{ns}/{name}'.format(**ds)

    def ds2url(self, ds):
        if not ds:
            return ''

        url = 'http://%s'%self.args.etl_api

        if isinstance(ds, str):
            return url + '/%s' % ds

        url += '/%s' % ds.backend
        url += '/%s' % ds.ns

        if ds.get('name'):
            url += '/%s' % ds.name

        return url

    def get_api(self, path='', **kw):
        url = 'http://%s%s' % (self.args.etl_api, '/%s'%path if path else '')
        return PRFRequest(url, **kw)

    def get_job_status_api(self, **kw):
        url = 'http://%s/job_status'%self.args.etl_api
        return PRFRequest(url, **kw)

    def get_job_status(self, **params):
        job_uid = params.get('uid', None)
        api = self.get_job_status_api()

        if job_uid == 'last':
            params['_sort']='-uid'

        resp = api.get(params=params)
        if resp.ok:
            data = resp.json()['data']
            if job_uid:
                return data[0]
            else:
                return data
        else:
            log.error('Job api failed with:', resp.text)

    def poll_job(self, job_uid, params):
        print('polling uid=%s' % job_uid)
        print('poll interval is %s seconds\n' % params.args['poll_interval'])

        while True:
            data = self.get_job_status(uid=job_uid)
            if data:
                workers = data.get('1_workers')
                links = data.get('3_links')

                if 'queued' not in workers and 'started' not in workers:
                    print('DONE')
                    return True

                print('PROGRESS(S/T)\t{sprogress} / {tprogress}  T={total} ({time_left} left at {sspeed})'\
                                                        .format(**data.get('0_counters')))
                print('WORKERS\t\t%s' % workers)
                for name, link in links.items():
                    print('%s\t%s' % (name.upper().ljust(10), link))
                print('TARGET\t\t%s' % data.get('2_datasets')['target'])
                print()
            else:
                return False

            time.sleep(params.args['poll_interval'])

        return True

    def query_source(self, params, **query):
        query = slovar(query).flat()
        ds = params.unflat().get('source')
        if not ds:
            return

        if ds.backend == 'http':
            return

        url = ('{backend}/{ns}/{name}').format(**ds)
        url = '%s/%s' % (self.args.etl_api, url)
        api = PRFRequest(url, auth=params.get('auth'))
        print('Querying %s' % api.base_url)
        pp(query)
        resp = api.get(params=query)
        return api.get_data(resp)

    def show_info(self, params, path='', info=''):

        def format_ds(params):
            fmt_dict = params.unflat().with_defaults(backend='mongo')
            fmt_dict['dot'] = '.' if fmt_dict.get('ns') else ''
            try:
                return '{backend}://{ns}{dot}{name}'.format(**fmt_dict)
            except KeyError as e:
                log.warning(e)
                return 'NA'

        print('JOB:', self.api.prepare_url(path))

        args = params.extract('args.*')

        if args.show_args:
            pp(args)

        _info = slovar({'args':[]})

        if args.dry:
            _info['args'].append('DRY')

        if args.dev:
            _info['args'].append('DEV')

        if args.drop:
            _info['args'].append('DROP')

        if 'target' in params:
            _info['op'] = params.target.get('op', '')

        _one_liner = []

        for ds, par in self._datasets.items():
            _info[ds] = format_ds(par)
            _one_liner += ['\n%s:' % ds.upper(), self.ds2url(par)]

        if 'merger' in params:

            _one_liner += ['\nM-DIRECTION: ' + params.merger['merge_direction']]

        essential_args = slovar()
        for kk,vv in params.extract('-args').flat().items():
            if 'query' in kk:
                essential_args[kk] = vv

        if args.get('arg'):
            essential_args['args'] = args.arg

        essential_args['tf'] = args.tf

        essential_args.update(args.extract(['str', 'mtr', 'mrules', 'mtr_post_merge']).pop_by_values([None]))

        print('ARGS:')
        for kk, vv in essential_args.items():
            print('%s=%s' % (kk,vv))

        print(' '.join(_one_liner))

        if not args.no_count:
            self.source_total = self.get_source_total(params)

        print('OP: ', args.get('o'))
        print('MODE: ', _info.args)
        print('INFO: ', args.get('info') or info or 'NO INFO?')
        print('TOTAL: %s' % self.source_total)

        print()

    def get_source_total(self, params):
        qlimit = params.source.flat().asint('query._limit', allow_missing=True)
        res = self.query_source(params, _count=1, **params.unflat('source')\
                                                .extract('source.query.*'))

        if isinstance(res, int):
            if qlimit:
                return min(qlimit, res)
            else:
                return res
        elif qlimit:
            return qlimit
        elif res == slovar():
            log.error('source is depleted')
            return 0
        else:
            self.error('Could not determine total in source')

        return None

    def get_datasets(self, ns):
        data = self.get_api('ns/%s/datasets' % ns).get().json()
        for each in data['data']:
            yield each.split('/')[-1]

    def drop_ds(self, ds_url):
        '''
            drops a dataset if it exists
        '''
        try:
            api = PRFRequest(ds_url)
            #todo does not work as intended - Solved
            resp = api.delete()
            if resp.ok:
                print('target `%s` dropped' % api.base_url)
            else:
                self.error('error dropping target %s' % resp.text)
        except Exception as e:
            log.error('Exception raised deleting datasets', ds_url, e)

    def _post(self, path, params):
        #TODO: simetimes unflat does not unflatten everything. do flat-unflat.
        job = self.api.post(path, data = params.flat().unflat())
        self.job_uid = job.json()['resource']['uid']
        return job

    def _run(self, path, params):
        self._post(path, params)
        self.poll_job(self.job_uid, params)
        return self.job_uid

    def _exit(self, *args, **kw):
        log.error(*args, **kw)

    def ns2url(self, backend, ns):
        return '%s/%s' % (backend, ns)

    def error(self, msg=''):
        raise CLIError(msg)

    def ls_ns(self, backend, ns):
        datasets = []
        ns_url = self.ns2url(backend, ns)

        if self.args.lsflat:
            ns_url += '?_flat=1'

        resp = self.get_api(ns_url).get()

        if not resp.ok:
            self.error('`%s` namespace not found or bad: %s' % (resp.url, resp.status_code))

        for each in resp.json()['data']:
            name = each.split(ns+'/')[-1]

            if backend == 'es':
                name = name.split('.')[-1]

            datasets.append(name)

        return datasets

    def get_etl_setting(self, name):
        data = self.get_api(_raise=True).get('/settings/%s' % name, params={'_flat':name}).json()
        if data:
            return data[name]

    def add_mongo_indices(self, args, name, indices):
        if args.skip_index:
            return

        if isinstance(args[name], list):
            ds_name = args[name][0]
        else:
            ds_name = args[name]

        ds = self.parse_ds(ds_name)
        if ds.backend != 'mongo':
            log.warning('`%s` is not a mongo dataset' % ds)
            return

        _indices = slovar()
        for ind in indices:
            op,_,margs = ind.partition('=')
            op = op or 'index'
            _indices.add_to_list(op, margs)

        uri = ''
        for kk, vv in _indices.items():
            uri += '%s=%s' % (kk,','.join(vv))

        args['mongo_index'].append('%s/%s?%s' % (ds.ns, ds.name, uri))
        return args

    def os_command(self, cmd, msg=''):
        if self.ask_input('RUN: %s ?' % (msg or cmd)):
            print('%sRUNNING: `%s`' % ('DRY: ' if self.args.dry else '', cmd))
            if not self.args.dry:
                os.system(cmd)
            print('Done')

    def run_mongo_indices(self, args):
        mongo_index = args.get('mongo_index', [])

        for mindex in mongo_index:
            cmd = 'prf.mongo_index ' + mindex
            if args.dry:
                print('DRY:', cmd)
            else:
                self.os_command(cmd)

    def might_drop_target(self, args):
        if not (args.drop or args.drop_ns):
            return

        tds = self.parse_ds(args.target)

        if args.drop_ns:
            tds.pop('name', None)

        ds_url = self.ds2url(tds)

        if not args.dry:
            if not args.silent:
                if input('DROP %s ? (y): ' % ds_url) == 'y':
                    self.drop_ds(ds_url)
                else:
                    print('skipped dropping the target %s' % ds_url)
            else:
                self.drop_ds(ds_url)

    def submit_job_request(self, args, path):
        return self._post(path, self.setup(args))

    def run(self, params, path='', info=''):
        f_params = slovar(params).flat()

        self.show_info(params, path=path, info=info)

        #if not target, read from source and print
        if not f_params.get('target.name'):
            query = params.extract('source.query.*', {'_limit':1})
            pp(self.query_source(f_params, **query))
            return

        def ask4workers():

            def _auto_workers():
                if not self.source_total:
                    return

                if params.get('workers'):
                    return params.workers

                ret = math.ceil(self.source_total/1000)
                if ret > 100:
                    return 100

                return ret

            if params.args.dev:
                print('Running in DEV mode')
                return

            elif not params.args.dry:
                aworkers = _auto_workers()

                if not params.args.silent:
                    workers = input('Enter number of workers (%s):' % aworkers)
                    if workers == '':
                        workers = aworkers
                else:
                    workers = aworkers

                workers = int(workers)
                if workers:
                    params['workers'] = workers
                    params['async'] = 1
                    print('Running with %s workers' % params['workers'])
                else:
                    params['async'] = 0
                    print('Running in DEV mode')

        if self.ask_input('RUN COMMAND ', ignore_enter=True):
            if not self.source_total:
                print('Empty Source')
                return

            self.run_mongo_indices(params.args)
            ask4workers()
            return self._run(path, params)
        else:
            print('Skipped\n')
