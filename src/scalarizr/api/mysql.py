'''
Created on Dec 04, 2011

@author: marat
'''
from __future__ import with_statement

import threading

from scalarizr import handlers, rpc
from scalarizr.services import mysql as mysql_svc


class MySQLAPI(object):
	"""
	@xxx: reporting is an anal pain
	"""

	error_messages = {
		'empty': "'%s' can't be blank",
		'invalid': "'%s' is invalid, '%s' expected"
	}


	def __init__(self):
		self._mysql_init = mysql_svc.MysqlInitScript()


	def grow_volume(self, volume, growth, async=False):
		self._check_invalid(volume, 'volume', dict)
		self._check_empty(volume.get('id'), 'volume.id')

		def do_grow():
			vol = storage2.volume(volume)
			self._mysql_init.stop('Growing data volume')
			try:
				growed_vol = vol.grow(**growth)
				return dict(growed_vol)
			finally:
				self._mysql_init.start()

		if async:
			txt = 'Grow MySQL data volume'
			op = handlers.operation(name=txt)
			def block():
				op.define()
				with op.phase(txt):
					with op.step(txt):
						data = do_grow()
				op.ok(data=data)
			threading.Thread(target=block).start()
			return op.id

		else:
			return do_grow()


	def _check_invalid(self, param, name, type_):
		assert isinstance(param, type_), self.error_messages['invalid'] % (name, type_)

	def _check_empty(self, param, name):
		assert param, self.error_messages['empty'] % name
