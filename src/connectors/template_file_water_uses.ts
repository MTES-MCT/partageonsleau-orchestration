import {type WaterUseCode} from './types.js'

type TemplateWaterUseDefinition = {
  code: WaterUseCode
  label: string
  aliases?: readonly string[]
}

export type TemplateWaterUseResolution = {
  code: WaterUseCode | undefined
  status: 'empty' | 'matched' | 'unknown'
  rawValue?: string
}

export const UNKNOWN_WATER_USE_CODE = '0'

const TEMPLATE_WATER_USES = [
  {code: '0', label: 'Usage inconnu', aliases: ['INCONNU']},
  {code: '1', label: 'Pas d’usage', aliases: ['PAS_D_USAGE']},
  {code: '2', label: 'Irrigation', aliases: ['IRRIGATION']},
  {code: '2A', label: 'Irrigation par aspersion'},
  {code: '2B', label: 'Irrigation gravitaire'},
  {code: '2C', label: 'Irrigation au goutte à goutte'},
  {code: '2D', label: 'Irrigation par tout autre procédé'},
  {code: '2E', label: 'Lutte antigel de cultures pérennes'},
  {code: '2F', label: 'Volume technique d’irrigation'},
  {
    code: '3',
    label: 'Agriculture-élevage (hors irrigation)',
    aliases: ['AGRICULTURE_ELEVAGE'],
  },
  {code: '3A', label: 'Abreuvage'},
  {code: '3B', label: 'Aquaculture', aliases: ['AQUACULTURE']},
  {code: '4', label: 'Industrie', aliases: ['INDUSTRIE']},
  {code: '4A', label: 'Agro-alimentaire'},
  {code: '4B', label: 'Industrie hors agro-alimentaire'},
  {code: '4C', label: 'Exhaure'},
  {
    code: '4D',
    label: 'Refroidissement avec restitution supérieure à 99 %',
    aliases: ['Refroidissement (> 99% de restitution)'],
  },
  {code: '5', label: 'Alimentation en eau potable (AEP)', aliases: ['AEP']},
  {code: '5A', label: 'Alimentation collective'},
  {code: '5B', label: 'Alimentation individuelle'},
  {code: '6', label: 'Énergie', aliases: ['ENERGIE']},
  {code: '6A', label: 'Pompe à chaleur'},
  {code: '6B', label: 'Géothermie'},
  {code: '6C', label: 'Refroidissement de centrales de production d’énergie'},
  {code: '6C1', label: 'Refroidissement de centrales thermiques'},
  {code: '6C2', label: 'Refroidissement de centrales nucléaires'},
  {
    code: '6C3',
    label: 'Refroidissement des centrales de production électrique',
  },
  {
    code: '6D',
    label: 'Barrages hydro-électriques - force motrice',
    aliases: ['Barrages hydro-électriques (force motrice)'],
  },
  {code: '7', label: 'Loisirs', aliases: ['LOISIRS']},
  {code: '7A', label: 'Bassin de natation', aliases: ['Piscine']},
  {code: '7B', label: 'Baignade'},
  {code: '7C', label: 'Autres activités de loisir'},
  {
    code: '7D',
    label: 'Arrosage',
    aliases: ['Arrosage (activités de loisir)'],
  },
  {code: '7E', label: 'Canon à neige'},
  {code: '8', label: 'Embouteillage', aliases: ['EMBOUTEILLAGE']},
  {
    code: '9',
    label: 'Thermalisme et thalassothérapie',
    aliases: ['THERMALISME_THALASSO'],
  },
  {code: '9A', label: 'Thermalisme'},
  {code: '9B', label: 'Thalassothérapie'},
  {
    code: '10',
    label: 'Défense contre incendie',
    aliases: ['DEFENSE_INCENDIE'],
  },
  {code: '11', label: 'Dépollution'},
  {
    code: '12',
    label: 'Réalimentation d’une ressource en eau',
    aliases: ['REALIMENTATION_EAU'],
  },
  {code: '12A', label: 'Soutien d’étiage'},
  {code: '12B', label: 'Compensation évaporation'},
  {code: '12C', label: 'Compensation irrigation'},
  {code: '12D', label: 'Compensation salubrité'},
  {code: '12E', label: 'Remplissage plan d’eau'},
  {code: '13', label: 'Canaux', aliases: ['CANAUX']},
  {code: '13A', label: 'Volume technique de navigation'},
  {code: '13B', label: 'Alimentation au soutien canal'},
  {code: '14', label: 'Soutien d’étiage', aliases: ['ETIAGE']},
  {code: '15', label: 'Entretien de voiries', aliases: ['ENTRETIEN_VOIRIES']},
  {
    code: '16',
    label: 'Alimentation au soutien canal',
    aliases: ['ALIMENTATION_SOUTIEN_CANAL'],
  },
  {code: '17', label: 'Usage domestique', aliases: ['DOMESTIQUE']},
] as const satisfies readonly TemplateWaterUseDefinition[]

export const TEMPLATE_WATER_USE_CODES: ReadonlySet<string> = new Set(
  TEMPLATE_WATER_USES.map(({code}) => code),
)

function normalizeTemplateWaterUseLabel(value: string): string {
  return value
    .trim()
    .normalize('NFD')
    .replaceAll(/\p{Diacritic}/gv, '')
    .replaceAll(/[’‘`´]/gv, "'")
    .replaceAll(/[‐‑‒–—]/gv, '-')
    .replaceAll(/\s*%\s*/gv, '%')
    .replaceAll(/\s+/gv, ' ')
    .toLocaleUpperCase('fr-FR')
}

function buildUnambiguousAliasMap(): ReadonlyMap<string, WaterUseCode> {
  const codesByAlias = new Map<string, Set<WaterUseCode>>()

  for (const definition of TEMPLATE_WATER_USES) {
    const aliases = 'aliases' in definition ? definition.aliases : []

    for (const alias of [definition.label, ...aliases]) {
      const normalizedAlias = normalizeTemplateWaterUseLabel(alias)
      const codes = codesByAlias.get(normalizedAlias) ?? new Set<WaterUseCode>()
      codes.add(definition.code)
      codesByAlias.set(normalizedAlias, codes)
    }
  }

  return new Map(
    [...codesByAlias]
      .filter(([, codes]) => codes.size === 1)
      .map(([alias, codes]) => [alias, [...codes][0]]),
  )
}

const WATER_USE_CODE_BY_ALIAS = buildUnambiguousAliasMap()
const CODE_PREFIX_PATTERN =
  /^(\d{1,2}(?:[A-Z](?:\d+)?)?)(?:\s*(?:-|–|—|·|:)\s*.+)?$/v

export function resolveTemplateWaterUse(
  rawUsage: unknown,
): TemplateWaterUseResolution {
  if (typeof rawUsage !== 'string' && typeof rawUsage !== 'number') {
    return {code: undefined, status: 'empty'}
  }

  const rawValue = String(rawUsage).trim()

  if (!rawValue) {
    return {code: undefined, status: 'empty'}
  }

  const normalizedValue = normalizeTemplateWaterUseLabel(rawValue)
  const prefixedCode = CODE_PREFIX_PATTERN.exec(normalizedValue)?.[1]

  if (prefixedCode && TEMPLATE_WATER_USE_CODES.has(prefixedCode)) {
    return {code: prefixedCode, status: 'matched'}
  }

  const aliasedCode = WATER_USE_CODE_BY_ALIAS.get(normalizedValue)

  if (aliasedCode) {
    return {code: aliasedCode, status: 'matched'}
  }

  return {
    code: UNKNOWN_WATER_USE_CODE,
    status: 'unknown',
    rawValue,
  }
}
