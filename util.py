import operator
import SPARQLWrapper as sp
import datetime
import pandas as pd
import urllib.request

class Mandaten():
	sparql = sp.SPARQLWrapper(endpoint='https://centrale-vindplaats.lblod.info/sparql',agent='Mozilla/5.0.0')
	# hier moet iets gebeuren met sparql.setHTTPAuth() of sparql.setCredentials() denk ik
	sparql.setReturnFormat(sp.JSON)

	def try_out(self):
		start = datetime.datetime.now()
		print(start)
		req = urllib.request.Request('https://centrale-vindplaats.lblod.info/sparql?query=%0A%09%09PREFIX+xsd%3A+%3Chttp%3A//www.w3.org/2001/XMLSchema%23%3E%0A%09%09PREFIX+fo%3A+%3Chttp%3A//www.w3.org/1999/XSL/Format%23%3E%0A%09%09PREFIX+foaf%3A+%3Chttp%3A//xmlns.com/foaf/0.1/%3E%0A%09%09PREFIX+org%3A+%3Chttp%3A//www.w3.org/ns/org%23%3E%0A%09%09PREFIX+skos%3A+%3Chttp%3A//www.w3.org/2004/02/skos/core%23%3E%0A%09%09PREFIX+besluit%3A+%3Chttp%3A//data.vlaanderen.be/ns/besluit%23%3E%0A%09%09PREFIX+mandaat%3A+%3Chttp%3A//data.vlaanderen.be/ns/mandaat%23%3E%0A%09%09PREFIX+persoon%3A+%3Chttp%3A//data.vlaanderen.be/ns/persoon%23%3E%0A%09%09PREFIX+generiek%3A+%3Chttps%3A//data.vlaanderen.be/ns/generiek%23%3E%0A%09%09PREFIX+ma%3A+%3Chttp%3A//www.w3.org/ns/ma-ont%23%3E%0A%09%09PREFIX+rdfs%3A+%3Chttp%3A//www.w3.org/2000/01/rdf-schema%23%3E%0A%09%09PREFIX+http%3A+%3Chttp%3A//www.w3.org/2011/http%23%3E%0A%09%09SELECT+distinct+%2A%0A%09%09WHERE+%7B%0A%09%09%09%3Fmandataris+mandaat%3AisBestuurlijkeAliasVan+%3Fpersoon.%0A%09%09%09%09%3Fpersoon+persoon%3AgebruikteVoornaam+%3Fvoornaam+.%0A%09%09%09%09%3Fpersoon++foaf%3AfamilyName+%3Fachternaam+.%0A%09%09%09++optional%7B%3Fpersoon+foaf%3Aname+%3Froepnaam+.%7D%0A%09%09%09%3Fmandataris+org%3Aholds+%3Fmandaat+.%0A%09%09%09%3Fmandaat+org%3Arole+%3Fbestuursfunctie+.%0A%09%09%09%3Fbestuursfunctie+skos%3AprefLabel+%3FbestuursfunctieLabel+.%0A%09%09%09++%3Fbestuursorgaanintijd+a+besluit%3ABestuursorgaan+.%0A%09%09%09++%3Fbestuursorgaanintijd+org%3AhasPost+%3Fmandaat+.%0A%09%09%09%3Fbestuursorgaanintijd+mandaat%3AisTijdspecialisatieVan+%3Fbestuursorgaan+.%0A%09%09%09++%3Fbestuursorgaanintijd+mandaat%3AbindingStart+%3FbestuursPeriodeStart+.%0A%09%09%09++bind%28year%28%3FbestuursPeriodeStart%29+as+%3FstartJaar%29+.%0A%09%09%09+OPTIONAL+%7B+%3Fbestuursorgaanintijd+mandaat%3AbindingEinde+%3FbestuursPeriodeEinde+%7D+++%0A%09%09%09++%3Fbestuursorgaan+skos%3AprefLabel+%3FbestuursorgaanLabel+.%0A%09%09%09++%3Fbestuursorgaan++besluit%3Aclassificatie+%3FbestuursorgaanClassificatie+.%0A%09%09%09++%3FbestuursorgaanClassificatie+skos%3AprefLabel+%3FbestuursorgaanClassificatieLabel+.%0A%09%09%09++%3Fbestuursorgaan+besluit%3Abestuurt+%3Fbestuurseenheid.%0A%09%09%09++%3Fbestuurseenheid+skos%3AprefLabel+%3FbestuurseenheidLabel.%0A%09%09%09+++%3Fbestuursorgaan+besluit%3Abestuurt+%3Fwerkingsgebied.%0A%09%09%09++%3Fwerkingsgebied+skos%3AprefLabel+%3Fwerkingsgebeidlabel.%0A%09%09%09%3Fbestuurseenheid++besluit%3Aclassificatie+%3FbestuurseenheidClassificatie+.%0A%09%09%09++%3FbestuurseenheidClassificatie+skos%3AprefLabel+%3FbestuurseenheidClassificatieLabel+.%0A%09%09%09%3Fmandataris+mandaat%3Astart+%3Fstart.%0A%09%09%09++OPTIONAL+%7B%3Fmandataris+mandaat%3Aeinde+%3Feind.%7D%0A%09%09%09++OPTIONAL+%7B%3Fmandataris+mandaat%3Astatus+%3Fstatus.%0A%09%09%09++++%3Fstatus+skos%3AprefLabel+%3FstatusLabel.%7D%0A%09%09%09++OPTIONAL+%7B%3Fmandataris+mandaat%3Abeleidsdomein+%3FbeleidsDomein.%0A%09%09%09++%3FbeleidsDomein+skos%3AprefLabel+%3FbeleidsDomeinLabel.%7D%0A%09%09%09++OPTIONAL+%7B%3Fmandataris+mandaat%3Arangorde+%3Frangorde.%7D%0A%09%09%09+++OPTIONAL+%7B%3Fmandataris+org%3AhasMembership+%3Ffractie.+%0A%09%09%09++%3Ffractie+org%3Aorganisation+%3Forganisatie+.%0A%09%09%09++++%3Forganisatie++%3Chttps%3A//www.w3.org/ns/regorg%23legalName%3E++%3Ffractienaam%7D%0A%09%09%09++FILTER+%28%3FstartJaar+%3E+2018%29%0A%09%09+++++%7D%0A%09%09order+by+%3Fpersoon+%3FbestuursorgaanClassificatieLabel%0A%09%09%09%09&format=json&output=json&results=json')
		stop = datetime.datetime.now()
		print(stop)
		runtime = stop - start
		print(f'runtime: {runtime} seconds')
		print(req.data)
		return req

	def bestuursorganen(self):
		start = datetime.datetime.now()
		print(f'gestart met bestuursorganen op te vragen\nstart: {start}')
		self.sparql.setReturnFormat(sp.JSON)
		self.sparql.setQuery("""
		PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
		PREFIX fo: <http://www.w3.org/1999/XSL/Format#>
		PREFIX foaf: <http://xmlns.com/foaf/0.1/>
		PREFIX org: <http://www.w3.org/ns/org#>
		PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
		PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
		PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
		PREFIX persoon: <http://data.vlaanderen.be/ns/persoon#>
		PREFIX generiek: <https://data.vlaanderen.be/ns/generiek#>
		PREFIX ma: <http://www.w3.org/ns/ma-ont#>
		PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
		PREFIX http: <http://www.w3.org/2011/http#>
		SELECT distinct ?bestuursorgaanClassificatieLabel
		WHERE {
		
					?mandataris org:holds ?mandaat .
		 			?bestuursorgaanintijd org:hasPost ?mandaat .
					?bestuursorgaanintijd mandaat:isTijdspecialisatieVan ?bestuursorgaan .
					?bestuursorgaan skos:prefLabel ?bestuursorgaanLabel .
					?bestuursorgaan  besluit:classificatie ?bestuursorgaanClassificatie .
					?bestuursorgaanClassificatie skos:prefLabel ?bestuursorgaanClassificatieLabel .
		     }
			order by ?bestuursorgaanClassificatieLabel""")
		result = self.sparql.query().convert()
		bestuursorganen_ugly = pd.DataFrame.from_dict(result['results']['bindings'])
		bestuursorganen = bestuursorganen_ugly.applymap(operator.itemgetter('value'))
		lst = bestuursorganen.squeeze()
		stop = datetime.datetime.now()
		print(f'bestuursorganen opgehaald\nstop: {stop}\nruntime: {stop-start} seconden\n{bestuursorganen}')
		return lst

	def master_query(self):
		start = datetime.datetime.now()
		print(start)
		masterframe = pd.DataFrame()
		bestuursorganen = self.bestuursorganen()
		for bestuursorgaan in bestuursorganen:
			if not bestuursorgaan == 'bestuursorgaanClassificatieLabel':
				print(bestuursorgaan)
				start_bestuursorgaan = datetime.datetime.now()
				print(f'gestart met masterquery voor {bestuursorgaan}\nstart: {start_bestuursorgaan}')
				self.sparql.setReturnFormat(sp.JSON)
				self.sparql.setQuery("""
				PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
				PREFIX fo: <http://www.w3.org/1999/XSL/Format#>
				PREFIX foaf: <http://xmlns.com/foaf/0.1/>
				PREFIX org: <http://www.w3.org/ns/org#>
				PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
				PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
				PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
				PREFIX persoon: <http://data.vlaanderen.be/ns/persoon#>
				PREFIX generiek: <https://data.vlaanderen.be/ns/generiek#>
				PREFIX ma: <http://www.w3.org/ns/ma-ont#>
				PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
				PREFIX http: <http://www.w3.org/2011/http#>
				SELECT distinct *
				WHERE {
					?mandataris mandaat:isBestuurlijkeAliasVan ?persoon.
						?persoon persoon:gebruikteVoornaam ?voornaam .
						?persoon  foaf:familyName ?achternaam .
					  optional{?persoon foaf:name ?roepnaam .}
					?mandataris org:holds ?mandaat .
					?mandaat org:role ?bestuursfunctie .
					?bestuursfunctie skos:prefLabel ?bestuursfunctieLabel .
					  ?bestuursorgaanintijd a besluit:Bestuursorgaan .
					  ?bestuursorgaanintijd org:hasPost ?mandaat .
					?bestuursorgaanintijd mandaat:isTijdspecialisatieVan ?bestuursorgaan .
					  ?bestuursorgaanintijd mandaat:bindingStart ?bestuursPeriodeStart .
					  bind(year(?bestuursPeriodeStart) as ?startJaar) .
					 OPTIONAL { ?bestuursorgaanintijd mandaat:bindingEinde ?bestuursPeriodeEinde }   
					  ?bestuursorgaan skos:prefLabel ?bestuursorgaanLabel .
					  ?bestuursorgaan  besluit:classificatie ?bestuursorgaanClassificatie .
					  ?bestuursorgaanClassificatie skos:prefLabel ?bestuursorgaanClassificatieLabel . """
				        + """FILTER (  ?bestuursorgaanClassificatieLabel = """ + "'" + bestuursorgaan + "')." +
					  """"?bestuursorgaan besluit:bestuurt ?bestuurseenheid.
					  ?bestuurseenheid skos:prefLabel ?bestuurseenheidLabel.
					   ?bestuursorgaan besluit:bestuurt ?werkingsgebied.
					  ?werkingsgebied skos:prefLabel ?werkingsgebeidlabel.
					?bestuurseenheid  besluit:classificatie ?bestuurseenheidClassificatie .
					  ?bestuurseenheidClassificatie skos:prefLabel ?bestuurseenheidClassificatieLabel .
					?mandataris mandaat:start ?start.
					  OPTIONAL {?mandataris mandaat:einde ?eind.}
					  OPTIONAL {?mandataris mandaat:status ?status.
					    ?status skos:prefLabel ?statusLabel.}
					  OPTIONAL {?mandataris mandaat:beleidsdomein ?beleidsDomein.
					  ?beleidsDomein skos:prefLabel ?beleidsDomeinLabel.}
					  OPTIONAL {?mandataris mandaat:rangorde ?rangorde.}
					   OPTIONAL {?mandataris org:hasMembership ?fractie.
					  ?fractie org:organisation ?organisatie .
					    ?organisatie  <https://www.w3.org/ns/regorg#legalName>  ?fractienaam}
					  FILTER (?startJaar > 2018)
				     }
				order by ?persoon ?bestuursorgaanClassificatieLabel
						""")
				result = self.sparql.query().convert()
				masterquery_mdb_ugly = pd.DataFrame.from_dict(result['results']['bindings'])
				masterquery_mdb = masterquery_mdb_ugly.applymap(operator.itemgetter('value'))
				stop_bestuursorgaan = datetime.datetime.now()
				print(f'Alles opgehaald voor {bestuursorgaan}\nstop: {stop_bestuursorgaan}\nruntime: {stop_bestuursorgaan-start_bestuursorgaan}seconden\n{masterquery_mdb.info()}')
			else:
				print(f'voor het label {bestuursorgaan} gaan we niets ophalen')
				break


		stop = datetime.datetime.now()
		print(stop)
		return masterframe

if __name__ == '__main__':
	mandatendatabank = Mandaten()
	result = mandatendatabank.master_query()