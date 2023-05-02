import util
import datetime

if __name__ == '__main__':
	mandatendatabank = util.Mandaten()
	query = mandatendatabank.query()
	query.to_csv(f'mandatendatabank_{datetime.date.today()}.csv')
	print(query.describe())