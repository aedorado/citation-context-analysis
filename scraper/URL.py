import urllib2
import time
import ssl
from log import *

class URL:

	def __init__(self, url):
		self.url = url

	def open(self):
		# logging.debug('Opening connection')
		OK = False
		tries = 0
		while not OK:
			try:
				self.response = urllib2.urlopen(self.url)
				self.finalurl = self.response.geturl()
				# logging.debug('Successfully connected to : ' + self.url)
				# if (self.redirect_occured()):
					# logging.debug('Redirected to : ' + self.finalurl)
				OK = True
			except urllib2.HTTPError as e:
				tries = tries + 1
				if tries >= 256:
					return -1
				if (e.code == 404):
					OK = True
				logging.error('Failure.\nAn HTTP error occured : ' + str(e.code) + ' for the url ' + self.url)
				logging.debug('Refetching : ' + self.url)
				time.sleep(1)
	
	def status_ok(self):
		try:
			self.response
		except AttributeError:
			return False
		else:
			return True

	def get_citations(self):
		self.citations_url = self.url.replace('summary', 'citations')
		OK = False
		tries = 0
		while not OK:
			try:
				self.response = urllib2.urlopen(self.citations_url)
				# logging.debug('Successfully connected to : ' + self.url)
				OK = True
				return self.response.read()
			except urllib2.HTTPError as e:
				tries = tries + 1
				if tries >= 256:
					return -1
				print str(e.code)
				logging.error('Failure.\nAn HTTP error occured : ' + str(e.code))
				logging.debug('Refetching : ' + self.url)
				time.sleep(1)

	def redirect_occured(self):
		return (self.url != self.finalurl)
		
	def get_doi(self):
		return self.finalurl[self.finalurl.find('?') + 5:]
		
	def get_url(self):
		return self.url
		
	def get_redirect_url(self):
		return self.finalurl

	def fetch(self):
		if (self.redirect_occured()):
			self.keep = True
			return self.response.read()
		else:
			self.keep = False
			return False
	    