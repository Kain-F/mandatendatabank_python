import operator
import SPARQLWrapper as sp
import datetime
import pandas as pd
import urllib.request
import numpy as np

class Mandaten():
	sparql = sp.SPARQLWrapper(endpoint='https://centrale-vindplaats.lblod.info/sparql')
	# hier moet iets gebeuren met sparql.setHTTPAuth() of sparql.setCredentials() denk ik
	sparql.setReturnFormat(sp.JSON)

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

	@property
	def query_BCSCD(self):
		start = datetime.datetime.now()
		print(start)
		masterframe = pd.DataFrame()
		start_bestuursorgaan = datetime.datetime.now()
		print(f'gestart met masterquery voor BCSD\nstart: {start_bestuursorgaan}')
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
				  ?bestuursorgaanClassificatie skos:prefLabel ?bestuursorgaanClassificatieLabel . 
			    FILTER ( ?bestuursorgaanClassificatieLabel = 'Bijzonder Comité voor de Sociale Dienst') .
				  ?bestuursorgaan besluit:bestuurt ?bestuurseenheid.
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
		masterquery_mdb_ugly_nan = pd.DataFrame.from_dict(result['results']['bindings'])
		xvalue = {'value': 'leeg'}
		df = pd.DataFrame()
		for kolom in masterquery_mdb_ugly_nan.T.index:
			df[kolom] = masterquery_mdb_ugly_nan[kolom].apply(lambda x: x if x == x else xvalue)
		masterquery_mdb = df.applymap(operator.itemgetter('value'))
		stop_bestuursorgaan = datetime.datetime.now()
		print(f'Alles opgehaald voor BCSD\nstop: {stop_bestuursorgaan}\nruntime: {stop_bestuursorgaan-start_bestuursorgaan}seconden\n{masterquery_mdb.info()}')

		stop = datetime.datetime.now()
		print(stop)
		return masterquery_mdb

	@property
	def query_GR(self):
		start = datetime.datetime.now()
		print(start)
		masterframe = pd.DataFrame()
		start_bestuursorgaan = datetime.datetime.now()
		print(f'gestart met masterquery voor GR\nstart: {start_bestuursorgaan}')
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
				  ?bestuursorgaanClassificatie skos:prefLabel ?bestuursorgaanClassificatieLabel . 
			    FILTER ( ?bestuursorgaanClassificatieLabel = 'Gemeenteraad') .
				  ?bestuursorgaan besluit:bestuurt ?bestuurseenheid.
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
		masterquery_mdb_ugly_nan = pd.DataFrame.from_dict(result['results']['bindings'])
		xvalue = {'value': 'leeg'}
		df = pd.DataFrame()
		for kolom in masterquery_mdb_ugly_nan.T.index:
			df[kolom] = masterquery_mdb_ugly_nan[kolom].apply(lambda x: x if x == x else xvalue)
		masterquery_mdb = df.applymap(operator.itemgetter('value'))
		stop_bestuursorgaan = datetime.datetime.now()
		print(f'Alles opgehaald voor GR\nstop: {stop_bestuursorgaan}\nruntime: {stop_bestuursorgaan-start_bestuursorgaan}seconden\n{masterquery_mdb.info()}')

		stop = datetime.datetime.now()
		print(stop)
		return masterquery_mdb

	@property
	def query_burgemeester(self):
		start = datetime.datetime.now()
		print(start)
		masterframe = pd.DataFrame()
		start_bestuursorgaan = datetime.datetime.now()
		print(f'gestart met masterquery voor Burgemeester\nstart: {start_bestuursorgaan}')
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
				  ?bestuursorgaanClassificatie skos:prefLabel ?bestuursorgaanClassificatieLabel . 
			    FILTER ( ?bestuursorgaanClassificatieLabel = 'Burgemeester') .
				  ?bestuursorgaan besluit:bestuurt ?bestuurseenheid.
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
		masterquery_mdb_ugly_nan = pd.DataFrame.from_dict(result['results']['bindings'])
		xvalue = {'value': 'leeg'}
		df = pd.DataFrame()
		for kolom in masterquery_mdb_ugly_nan.T.index:
			df[kolom] = masterquery_mdb_ugly_nan[kolom].apply(lambda x: x if x == x else xvalue)
		masterquery_mdb = df.applymap(operator.itemgetter('value'))
		stop_bestuursorgaan = datetime.datetime.now()
		print(f'Alles opgehaald voor Burgemeester\nstop: {stop_bestuursorgaan}\nruntime: {stop_bestuursorgaan-start_bestuursorgaan}seconden\n{masterquery_mdb.info()}')

		stop = datetime.datetime.now()
		print(stop)
		return masterquery_mdb

	@property
	def query_CBS(self):
		start = datetime.datetime.now()
		print(start)
		masterframe = pd.DataFrame()
		start_bestuursorgaan = datetime.datetime.now()
		print(f'gestart met masterquery voor CBS\nstart: {start_bestuursorgaan}')
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
				  ?bestuursorgaanClassificatie skos:prefLabel ?bestuursorgaanClassificatieLabel . 
			    FILTER ( ?bestuursorgaanClassificatieLabel = 'College van Burgemeester en Schepenen') .
				  ?bestuursorgaan besluit:bestuurt ?bestuurseenheid.
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
		masterquery_mdb_ugly_nan = pd.DataFrame.from_dict(result['results']['bindings'])
		xvalue = {'value': 'leeg'}
		df = pd.DataFrame()
		for kolom in masterquery_mdb_ugly_nan.T.index:
			df[kolom] = masterquery_mdb_ugly_nan[kolom].apply(lambda x: x if x == x else xvalue)
		masterquery_mdb = df.applymap(operator.itemgetter('value'))
		stop_bestuursorgaan = datetime.datetime.now()
		print(f'Alles opgehaald voor CBS\nstop: {stop_bestuursorgaan}\nruntime: {stop_bestuursorgaan-start_bestuursorgaan}seconden\n{masterquery_mdb.info()}')

		stop = datetime.datetime.now()
		print(stop)
		return masterquery_mdb


if __name__ == '__main__':
	mandatendatabank = Mandaten()
	bestuursorganen = mandatendatabank.bestuursorganen()
	BCSD = mandatendatabank.query_BCSCD
	GR = mandatendatabank.query_GR
	Burgemeester = mandatendatabank.query_burgemeester
	CBS = mandatendatabank.query_CBS
	result = pd.concat([BCSD,GR,Burgemeester,CBS])
	result.to_csv(f'probeersel_mandatendatabank_{datetime.date.today()}')

	probeersel = mandatendatabank.query(bestuursorgaan='Bijzonder Comité voor de Sociale Dienst')
