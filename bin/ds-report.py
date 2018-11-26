import concurrent.futures
import os.path
import re
import sys
import xml.sax
from functools import reduce
from io import StringIO

class DsHandler(xml.sax.ContentHandler):
	def __init__(self,results):
		self.empty = ""
		self.pid = self.empty
		self.dsId = self.empty
		self.dsType = self.empty
		self.dsLocation = self.empty
		self.content = re.compile('content\.\d+', re.IGNORECASE)
		self.results = results
	def startElement(self, tag, attributes):
		# self.pid = /foxml:digitalObject@PID
		# self.dsType = //foxml:datastream@CONTROL_GROUP
		# self.dsId = //foxml:datastreamVersion[@ID ~ /^content/i]/@ID
		# self.dsLocation = //foxml:contentLocation@REF
		if tag == "foxml:digitalObject":
			self.pid = attributes["PID"]
		elif tag == "foxml:datastream":
			self.dsType = attributes["CONTROL_GROUP"]
		elif tag == "foxml:datastreamVersion":
			self.dsId = attributes["ID"]
		elif tag == "foxml:contentLocation":
			self.dsLocation = attributes["REF"]
	def endElement(self, tag):
		if tag == "foxml:datastreamVersion":
			# check that dsId is content/CONTENT
			# check that dsType is E
			# log dsLocation info
			if self.content.match(self.dsId) and self.dsType == 'E':
				els = [self.pid] + self.dsId.split('.')
				els.append(self.dsLocation) 
				self.results.append(els)
			# reset data
			self.dsId = self.empty
			self.dsLocation = self.empty
		if tag == "foxml:datastream":
			self.dsType = self.empty
		elif tag == "foxml:digitalObject":
			self.pid = self.empty

def latest(el1, el2):
	if el1[1] == 'content' and el2[1] == 'CONTENT':
		return el1
	if el1[1] == 'CONTENT' and el2[1] == 'content':
		return el2
	if int(el1[2]) > int(el2[2]):
		return el1
	else:
		return el2

def parse(line):
	results = []
	rel_path = line.rstrip()
	if os.path.isfile(rel_path):
		parser = xml.sax.make_parser()
		parser.setContentHandler( DsHandler(results) )
		try:
			parser.parse(open(rel_path,'r'))
		except:
			#TODO: refactor this sloppy scoping
			print(rel_path, file=err, flush=True)
	if len(results) > 0:
		return ','.join(reduce(latest, results))
	else:
		return None


if ( __name__ == "__main__"):
	if len(sys.argv) < 2:
		print("call with filename of path list")
	else:
		if len(sys.argv) < 3:
			out = sys.stdout
		else:
			out = open(sys.argv[2],'w')
		if len(sys.argv) < 4:
			err = sys.stderr
		else:
			err = open(sys.argv[3],'w')
		print("pid,dsId,dsVersion,dsLocation", file=out)
		with concurrent.futures.ProcessPoolExecutor() as executor:
			for line in executor.map(parse, open(sys.argv[1],'r')):
				if line != None:
					print(line, file=out, flush=True)
