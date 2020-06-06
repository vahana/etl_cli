import logging
import os
from argparse import ArgumentParser
from slovar.strings import split_strip
from slovar import slovar
from prf.utils import qs2dict, urlify

from etl_cli.base import Base, Args

log = logging
log.basicConfig(level=logging.INFO)

OPS = ['create', 'index', 'update', 'upsert', 'delete', 'insert']

class Script(Base):

    MUTABLE_OPS = Base.MUTABLE_OPS + [
        'stra',
        'mkeys',
        'mq',
        'mtra',
        'query',
        'tf',
        'tq',
    ]
    def __init__(self):
        self.add_argument('-s', '--source', help='source dataset')
        self.add_argument('-t', '--target', help='target dataset')
        self.add_argument('--st', help='source and target')
        self.add_argument('--smt', help='source,merger and target')
        self.add_argument('--mt', help='merger and target')
        self.add_argument('--str', help='source transformer')
        self.add_argument('--stra', action='append', default=[], help='source transformer args')

        self.add_argument('--surl', help='url for http source')
        self.add_argument('--murl', help='url for http merger')
        self.add_argument('--turl', help='url for http target')

        self.add_argument('-m', '--merger', help='merger dataset name')
        self.add_argument('--mq', action='append', default=[], help='merger query')
        self.add_argument('--mmd', help='merge direction', choices=['s2m', 'm2s'])
        self.add_argument('--mtr', help='merger transformer')
        self.add_argument('--mtr-post-merge', help='merger transformer post merge')
        self.add_argument('--mtra', action='append', default=[], help='merge transformer args')
        self.add_argument('--mrm', help='merger.require_match')
        self.add_argument('--mrnm', help='merger.require_no_match')
        self.add_argument('--mmo', help='merger.match_one')
        self.add_argument('--mstrict', help='merger.strict_match')
        self.add_argument('--munwind', help='merger.unwind')
        self.add_argument('--mma', help='merger.merge_as')
        self.add_argument('--mrules', help='merger.merge_rules')
        self.add_argument('--mrules-scm', help='merger.merge_rules_scm')
        self.add_argument('--mkeys', help='merger query short hand')

        self.add_argument('--diff')
        self.add_argument('--diff-context')

        self.add_argument('--tf', help='target fields', default='')
        self.add_argument('--tq', help='target query', action='append')
        self.add_argument('--overwrite', help='target.overwrite param')
        self.add_argument('--skip_by', help='target.skip_by')
        self.add_argument('--skip-timestamp', help='target.skip_timestamp')

        self.add_argument('-q', '--query', action='append', default=[],
                                        help='source query can be specified multiple times')

        self.add_argument('--job-poll', help='poll the job by uid')
        self.add_argument('--job-run', help='run jobs', action='store_true')
        self.add_argument('--jq', help='query on jobs', action='append')

        self.add_argument('--tmap', help='target.mapping')
        self.add_argument('-o', help='operation on target')

        self.add_argument('--contid')
        self.add_argument('--lsflat', help='flat list of datasets', action='store_true')

        super(Script, self).__init__()
        self.api = self.get_api('jobs', _raise=True)
        self.args = self.expand_ds_args(self.args)

        if self.args.surl:
            self.args.source = 'http'
        if self.args.murl:
            self.args.merger = 'http'
        if self.args.turl:
            self.args.target = 'http'

    def expand_ds_args(self, args):
        if args.get('smt'):
            args['source'] = args['merger'] = args['target'] = args['smt']

        elif args.get('st'):
            args['source'] = args['target'] = args['st']

        elif args.get('mt'):
            args['merger'] = args['target'] = args['mt']

        return args

    def expand_ds(self, args):

        for ds in ['merger', 'target']:
            if not args.get(ds):
                continue

            if '*' in args[ds]:
                s_ds = self.parse_ds(args.source)
                trg = []

                for kk, vv in self.parse_ds(args[ds]).items():
                    if vv == '*':
                        vv = s_ds[kk]
                    trg.append(vv)

                args[ds] = '/'.join(trg)

        return args

    def setup_http_be(self, ds_name, args, params):
        ds = self.parse_ds(args[ds_name])

        url_arg = '%surl' % ds_name[0] #first letter of dataset e.g. `surl` for `source url`
        if args[url_arg]:
            params['%s.query._url' % ds_name] = args[url_arg]
            params['%s.ns' % ds_name] = ds.ns or args.ns or 'NA'
            params['%s.name' % ds_name] = ds.name or 'NA'
            params['source.query._limit'] = 1

    def setup_s3_be(self, args, params):
        ds = self.parse_ds(args['target'])

        if ds['backend'] == 's3':
            params['batch_size'] = params['target.write_buffer_size'] = 100000

    def setup_source(self, args, params):

        params.update(self.setup_ds(args.source, 'source', required=True).flat())

        if args.str:
            params.update(self.process_transformers(args.str, 'source.post_read'))

        params.update(self.qlsit2query(args.query, 'source.query.'))

        self.setup_http_be('source', args, params)

        return params

    def setup_merger(self, args, params):
        if not args.merger:
            return params

        params.update(self.setup_ds(args.merger, 'merger').flat())

        if args.mq:
            params.update(self.qlsit2query(args.mq, 'merger.query.'))

        if args.mmd:
            params['merger.merge_direction'] = args.mmd
        else:
            self.required('mmd')

        params['merger.require_match'] = args.get('mrm')
        params['merger.require_no_match'] = args.get('mrnm')
        params['merger.match_one'] = args.get('mmo')
        params['merger.strict_match'] = args.get('mstrict')
        params['merger.merge_as'] = args.get('mma')

        if args.mtr:
            params.update(self.process_transformers(args.mtr, 'merger.post_read'))

        if args.mtr_post_merge:
            params.update(self.process_transformers(args.mtr_post_merge, 'merger.post_merge'))

        if args.munwind is not None:
            params['merger.unwind'] = args.munwind
            params['merger.match_one'] = 0

        if args.mrules:
            params['merger.merge_rules'], _ = self.trans_name(args.mrules)
            params['merger.merge_rules_scm'] = args.mrules_scm or ''

        for mk in args.aslist('mkeys', default=[]):
            params['merger.query.%s' % mk] = '#%s#' % mk

        self.setup_http_be('merger', args, params)

        return params

    def setup_target(self, args, params):
        if not args.target:
            return params

        if args.log_ds:
            log_ds_name = args.target.replace('.', '-').replace('/', '-')
            params['target.log_ds'] = '%s.%s' % (args.log_ds, log_ds_name)

        params.update(self.setup_ds(args.target, 'target').flat())

        if not args.o:
            self.error('missing `o` - operation argument')

        arg_op = args.o
        if ':' not in args.o and args.pk:
            arg_op += ':%s' % args.pk

        op, _, op_arg = arg_op.partition(':')

        if op == 'insert':
            params['target.op'] = 'create'
            params['target.skip_by'] = op_arg

        else:
            params['target.op'] = arg_op

        params['target.fields'] = args.get('tf')
        params['target.overwrite'] = args.get('overwrite')
        params['target.skip_timestamp'] = args.get('skip_timestamp')

        if op not in OPS:
            self.error('bad `o` argument. choices: %s. got `%s` instead' % (OPS, op))

        if not args.pk and op_arg:
            params['target.pk'] = op_arg
            log.warning('pk is set to `%s` from op argument' % op_arg)
        elif args.pk:
            params['target.pk'] = args.pk
        else:
            log.warning('pk is missing for this backend')

        if params.get('target.backend') == 'mongo':
            if op in ['create', 'insert', 'upsert'] and params.get('target.pk'):
                self.add_mongo_indices(args, 'target', ['unique=%s' % params.get('target.pk')])

            if op == 'insert':
                log.warning('Make sure target has unique index created on `%s` field for `insert` to work!', op_arg)
        elif params.get('target.backend') == 'es':
            if args.get('tmap'):
                params['target.mapping']='smurfs.gazelle_es.%s' % args['tmap']
        elif params.get('target.backend') == 'csv':
            pass

        if args['tq']:
            params.update(self.qlsit2query(args.tq, 'target.query.'))

        self.setup_http_be('target', args, params)
        self.setup_s3_be(args, params)

        return params

    def setup_pagination(self, args, params):
        if args.get('paginate'):
            if params.get('source.backend') == 'mongo':
                params['_pagination'] = args.get('paginate')
            elif params.get('source.backend') == 'es':
                params['_pagination'] = 1
                params['workers'] = 1
                log.warning('pagination for ES backend must be run in dev mode or with 1 worker!')
            else:
                log.warning('paginate not supported for this backend: ignoring!')

        return params

    def setup(self, args, params=None):

        args = self.expand_ds(args)

        params = super(Script, self).setup(args, params)

        if args.job_poll or args.job_run:
            return params

        params = self.setup_source(args, params)
        params = self.setup_merger(args, params)
        params = self.setup_target(args, params)
        params = self.setup_pagination(args, params)

        params.args = args

        return params.pop_by_values([None, '']).unflat()

    def detect_self_updates(self, _args):

        if not _args.get('target'):
            return False

        t_ds = self.parse_ds(_args.target)

        for ds_name in ['source']:
            if not _args.get(ds_name):
                continue

            if self.parse_ds(_args.get(ds_name)) == t_ds:
                return True

        return False

    def base_run(self, params, path='', info=''):
        return super().run(params, path, info)

    def build_target_from_source(self, _args):
        trg = self.parse_ds(_args.target)
        src = self.parse_ds(_args.source)

        for part in ['ns', 'name']:
            if '*' in trg.get(part):
                src_val =  src[part]
                if part == 'name' and src[part].endswith('.csv'):
                    src_val = src[part].replace('.csv', '')

                trg[part] = trg[part].replace('*', src_val)

        return '%s/%s/%s' % (trg.backend, trg.ns, urlify(trg.name))

    def run(self, args, path='/etl', info='ETL'):
        _args = Args(args, self.MUTABLE_OPS)

        _run_in_loop = False

        def _run():
            params = self.setup(_args)
            if _args.get('job_poll'):
                return self.poll_job(_args.job_poll, params)

            return self.base_run(params, path=path, info=info)

        def _run_loop(job_uid):
            _args['query'] += ['logs.job.contid__ne=%s' % job_uid]
            _totals = []
            _loop_count = 1
            while(True):
                _totals.append(self.source_total)
                #if total reached 0 or did not change last 2 loops, then quit
                if _totals[-1] == 0 or len(_totals) > 2 and _totals[-1] == _totals[-2]:
                    return

                print('LOOP %s' % _loop_count)
                if not _run():
                    break
                _loop_count +=1

        if _args.get('contid') and _args.contid != 'self':
            _run_loop(_args['contid'])
            return

        if self.detect_self_updates(_args):
            print('WARNING: self-updating dataset detected. will run in LOOP')
            _run_in_loop = True

        _contid = _args.pop('contid', None)

        job_uid = _run()

        if not job_uid:
            return

        if _contid == 'self':
            _run_in_loop = True
            _args.contid = job_uid

        if _run_in_loop and not _args.dry:
            _args['silent'] = True
            _run_loop(job_uid)

    def ls_sources(self, source):
        sources = []

        if '*' not in source:
            return [source]

        ds = self.parse_ds(source)
        pref, suf = ds.name.split('*')
        for it in self.ls_ns(ds.backend, ds.ns):
            if it.startswith(pref) and it.endswith(suf):
                ds.name=it
                sources.append('%s/%s/%s' % (ds.backend, ds.ns, ds.name))

        return sources


def run():

    script = Script()
    _args = script.expand_ds(script.args.to_slovar())

    script.might_drop_target(_args)

    for source in script.ls_sources(_args.source or ''):
        _aa = _args.copy()
        _aa.source = source
        if _args.target and '*' in _args.target:
            _aa.target = script.build_target_from_source(_aa)

        script.run(_aa)
