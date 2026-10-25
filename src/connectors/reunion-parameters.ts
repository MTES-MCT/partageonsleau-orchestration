import {normalizeReunionText} from './reunion-values.js'

type UnitBounds = {min?: number; max?: number}
type Parameter = {aliases?: string[]; units: Record<string, UnitBounds>}
const flows = {'L/s': {min: 0, max: 15000}, 'm³/h': {min: 0, max: 60000}}
const volumes = {'m³': {min: 0, max: 30000000}}

/** Native units and validation ranges from the Réunion v0.0.11 format. */
export const reunionParameters: Record<string, Parameter> = {
  chlorures: {units: {'mg/L': {min: 0.01, max: 1000}}},
  conductivité: {
    aliases: ['conductivité électrique'],
    units: {'µS/cm': {min: 0.01, max: 2000}},
  },
  'débit prélevé': {units: flows},
  'débit réservé': {units: flows},
  'débit restitué': {units: flows},
  nitrates: {units: {'mg/L': {min: 0, max: 500}}},
  'niveau piézométrique': {
    aliases: ['niveau d’eau'],
    units: {'m NGR': {min: -200, max: 3000}},
  },
  pH: {units: {autre: {min: 4, max: 11}}},
  'relevé d’index de compteur': {units: {'m³': {min: 0, max: 1000000000}}},
  sulfates: {units: {'mg/L': {min: 0.01, max: 5000}}},
  température: {units: {'degrés Celsius': {min: 0.01, max: 40}}},
  turbidité: {units: {autre: {min: 0, max: 5000}}},
  'volume prélevé': {units: volumes},
  'volume restitué': {units: volumes},
  autre: {units: {autre: {}}},
}

export function reunionParameter(value: string): string | undefined {
  const normalized = normalizeReunionText(value)
  return Object.entries(reunionParameters).find(([name, config]) =>
    [name, ...(config.aliases ?? [])].some(
      (item) => normalizeReunionText(item) === normalized,
    ),
  )?.[0]
}

const units: Record<string, string[]> = {
  'µS/cm': ['us/cm', 'μs/cm', 'microsiemens/cm'],
  'degrés Celsius': ['°c', 'degre celsius', 'celsius', 'c'],
  'L/s': ['litres/s', 'litre/s', 'litres par seconde', 'litre par seconde'],
  'm³/h': ['metres cubes par heure', 'metre cube par heure', 'm3/heure'],
  'm³': ['metres cubes', 'metre cube'],
  'm NGR': ['metres ngr', 'metre ngr', 'mngr'],
  'mg/L': ['milligrammes par litre', 'milligramme par litre'],
  autre: ['autres'],
}

function unitKey(value: string): string {
  return normalizeReunionText(value)
    .replaceAll('³', '3')
    .replaceAll(/[µμ]/g, 'u')
}

export function reunionUnit(value: string): string | undefined {
  return Object.entries(units).find(([name, aliases]) =>
    [name, ...aliases].some((item) => unitKey(item) === unitKey(value)),
  )?.[0]
}
