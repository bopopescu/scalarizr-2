'''
Created on Feb 25, 2013

@author: uty
'''

import re
from scalarizr.util.cryptotool import pwgen
from scalarizr.services import postgresql as postgresql_svc
from scalarizr import rpc


OPT_REPLICATION_MASTER = postgresql_svc.OPT_REPLICATION_MASTER
__postgresql__ = postgresql_svc.__postgresql__


class PostgreSQLAPI(object):

    replication_status_query = '''SELECT
    CASE WHEN pg_last_xlog_receive_location() = pg_last_xlog_replay_location()
    THEN 0
    ELSE EXTRACT (EPOCH FROM now() - pg_last_xact_replay_timestamp()) END
    AS xlog_delay;
    '''

    @rpc.service_method
    def reset_password(self, new_password=None):
        """ Reset password for PostgreSQL user 'scalr'. Return new password """
        if not new_password:
            new_pass = pwgen(10)
        pg = postgresql_svc.PostgreSql()
        pg.root_user.change_role_password(new_pass)
        pg.root_user.change_system_password(new_pass)
        pg.reload()

        return new_pass

    def _parse_query_out(self, out):
        '''
        Parses xlog_delay or error string from strings like:
         log_delay
        -----------
                 034
        (1 row)

        and:
        ERROR:  function pg_last_xact_replay_timesxtamp() does not exist
        LINE 1: select pg_last_xact_replay_timesxtamp() as not_modified_sinc...
                       ^
        HINT:  No function matches the given name and argument...

        '''
        result = {'error': None, 'xlog_delay': None}
        error_match = re.search(r'ERROR:.*?\n', out)
        if error_match:
            result['error'] = error_match.group()
            return result

        diff_match = re.search(r'xlog_delay.+-\n *\d+', out, re.DOTALL)
        if not diff_match:
            #if no error and query returns nothing
            return result

        result['xlog_delay'] = diff_match.group().splitlines()[-1].strip()
        return result

    @rpc.service_method
    def replication_status(self):
        psql = postgresql_svc.PSQL()
        query_out = psql.execute(self.replication_status_query)
        query_result = self._parse_query_out(query_out)

        is_main = int(__postgresql__[OPT_REPLICATION_MASTER])

        if not query_result['xlog_delay']:
            if is_main:
                return {'main': {'status': 'up'}}
            return {'subordinate': {'status': 'down',
                              'error': query_result['error']}}
        return {'subordinate': {'status': 'up',
                          'xlog_delay': query_result['xlog_delay']}}
