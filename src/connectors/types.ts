export type ConfigEntry = {
	service_account: string;
};

export type ServiceAccountPointContext = {
	pointId: string;
	connector: string;
	lastRunAt: string | undefined;
};

export type ServiceAccountContext = {
	serviceAccount: string;
	points: ServiceAccountPointContext[];
};

export type ConnectorRunContext = {
	serviceAccount: string;
	pointId: string;
	lastRunAt: string | undefined;
};

export type TimeserieValue = {
	date: string;
	value: number;
};

export enum MetricType {
	INDEX = 'index',
	VOLUME_PRELEVE = 'volume_preleve',
}

export enum MetricUnit {
	M3 = 'm3',
}

export enum MetricFrequency {
	SECOND = 'second',
	MINUTE = 'minute',
	FIFTEEN_MINUTES = '15_minutes',
	HOUR = 'hour',
	DAY = 'day',
	WEEK = 'week',
	MONTH = 'month',
	YEAR = 'year',
}

export type Timeserie = {
	type: MetricType;
	frequency: MetricFrequency;
	values: TimeserieValue[];
	unit: MetricUnit | undefined;
};

export type ParsedPointPayload = {
	id_point_de_prelevement: string;
	metrics: Timeserie[];
};

export type ConnectorOutput = {
	connector: string;
	serviceAccount: string;
	pointId: string;
	generatedAt: string;
	data: ParsedPointPayload;
};
