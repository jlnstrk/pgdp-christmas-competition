# PGdP-Weihnachtswettbewerb [WS19/20]
## Aufgabenstellung:

TPC-H ist einer der wichtigsten Benchmarks im Datenbankbereich. Alle großen Datenbanksysteme optimieren auf diesen Benchmark. Um vergleichen zu können, wie gut eine Datenbank Anfragen bearbeitet, lohnt es sich Anfragen von Hand zu programmieren um einen Vergleichswert zu haben, was ein Datenbanksystem leisten könnte, wenn es keinen Overhead hat.

Ziel dieser Aufgabe ist es, dass Sie 3 Dateien einlesen, parsen und die Inhalte miteinander zu joinen um einfache Anfragen zu diesem Datensatz möglichst effizient beantworten zu können.

Die Anfragen, die Ihr Programm beantworten können soll, lauten wie folgt. Wie hoch ist die durchschnittliche Quantität der Lineitems einer Bestellung eines Kunden, der zu einem bestimmten Marketsegment gehört. Die Signatur der entsprechenden Methode lautet public long getAverageQuantityPerMarketSegment(String marketsegment).

Ihre Aufgabe ist es, Daten aus den drei mitgelieferten *.tbl Dateien einzulesen, die im baseDataDirectory liegen. Wie sie dies tun, ist Ihnen überlassen. Die Quantity der Lineitems parsen sie auch hier mit 100 multipliziert.

Implementierung
Sie sollen die Methode public long getAverageQuantityPerMarketSegment(String marketsegment) der Klasse Database implementieren, die als Parameter ein String marketsegment bekommt und zurückgibt, wie hoch die durchschnittliche LineItem Quantität pro Bestellung ist. Dabei speichern sie für das Berechnen des Durchschnitts die Anzahl und Gesamtmenge als long während der kompletten Rechnung und erst zum Schluss berechnen Sie den Quotienten dieser beiden Werte und geben diesen Wert als long zurück (ganzzahlige Division).

Regeln für den Wettbewerb
Ihre Implementierung muss mit einem beliebigem Datensatz der zum TPC-H Schema und der folgenden SQL Query passt zurechtkommen:
```
select cast(floor(avg(l_quantity)*100) as int)
from lineitem, orders, customer
where
 l_orderkey = o_orderkey and
 o_custkey = c_custkey and
 c_mktsegment = <Parameter>
 ```
Die Benchmarks werden mit einem Datensatz auf Scalefactor 1 gemacht (der Datensatz ist damit 1 GB groß)
Der Benchmark wird mehrere Anfragen an die Datenbank stellen
Der Scalefactor 1 Datensatz ist unter HyperDB testbar
Es wird einen Test geben, der den normalen SF 1 Datensatz leicht abändert, dieser Test wird nicht gebenchmarkt, muss aber bestanden werden !
Ihre Lösung muss den Datensatz wirklich einlesen und die Werte basierend auf den eingelesenen Werten berechnen.

### SF 1 Datensatz
https://db.in.tum.de/teaching/ws1617/foundationsde/sheets/data/lineitem.tbl.xz
https://db.in.tum.de/teaching/ws1617/foundationsde/sheets/data/orders.tbl.xz
https://db.in.tum.de/teaching/ws1617/foundationsde/sheets/data/customer.tbl.xz
