import type { SignalDefinition } from '@emis/types';

export const signalRegistry: SignalDefinition[] = [
  {
    key: 'trend_structure',
    family: 'trend',
    horizon: 'swing',
    description: 'Captures trend persistence, compression-expansion state, and breakout quality.',
    inputs: ['market_bars'],
    outputs: ['structure_score', 'trend_regime', 'breakout_quality'],
  },
  {
    key: 'oi_price_divergence',
    family: 'leverage',
    horizon: 'intraday',
    description: 'Measures divergence between price impulse and open-interest expansion or contraction.',
    inputs: ['market_bars', 'market_open_interest'],
    outputs: ['crowdedness_score', 'squeeze_probability'],
  },
  {
    key: 'funding_stress',
    family: 'leverage',
    horizon: 'intraday',
    description: 'Flags positioning crowding through funding extremes and basis stress.',
    inputs: ['market_funding'],
    outputs: ['funding_stress_score'],
  },
  {
    key: 'liquidation_magnet_distance',
    family: 'liquidation',
    horizon: 'intraday',
    description: 'Estimates distance to likely forced-flow liquidation clusters.',
    inputs: ['market_bars', 'market_liquidations'],
    outputs: ['liq_magnet_proximity'],
  },
  {
    key: 'macro_alignment',
    family: 'macro',
    horizon: 'daily',
    description: 'Scores alignment between the asset and macro drivers such as DXY, yields, and index leadership.',
    inputs: ['market_bars', 'macro_series_points'],
    outputs: ['macro_alignment_score'],
  },
];
