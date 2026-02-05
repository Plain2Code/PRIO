"""
Evaluate: ML Regime vs. rule-based Hurst filter.

Compares for all 27 pairs:
  1. Current ML regime prediction (from loaded models)
  2. Raw Hurst exponent value
  3. ADX value (already a strategy filter)
  4. Various Hurst threshold scenarios

Run: python scripts/evaluate_regime.py
"""

import asyncio
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()


async def main():
    from src.broker.capitalcom import CapitalComBroker
    from src.data.pipeline import DataPipeline
    from src.ml.regime import RegimeDetector

    # Load config
    with open("config/default.yaml") as f:
        config = yaml.safe_load(f)

    # Connect broker
    broker = CapitalComBroker(
        api_key=os.getenv("CAPITALCOM_API_KEY", ""),
        identifier=os.getenv("CAPITALCOM_IDENTIFIER", ""),
        password=os.getenv("CAPITALCOM_PASSWORD", ""),
        environment=os.getenv("CAPITALCOM_ENVIRONMENT", "demo"),
    )

    pipeline = DataPipeline(broker, config)
    pairs = config["trading"]["pairs"]
    regime_cfg = config.get("ml", {}).get("regime", {})

    # Load per-pair regime models
    models_dir = Path("models")
    detectors: dict[str, RegimeDetector] = {}
    for pair in pairs:
        pair_files = sorted(models_dir.glob(f"regime_{pair}_*.joblib"), reverse=True)
        if pair_files:
            det = RegimeDetector(regime_cfg)
            det.load_model(str(pair_files[0]))
            detectors[pair] = det

    print(f"\n{'='*90}")
    print(f"  REGIME FILTER EVALUIERUNG: ML vs. Hurst regelbasiert")
    print(f"  {len(detectors)} Regime-Modelle geladen, {len(pairs)} Paare")
    print(f"{'='*90}\n")

    # Header
    print(f"  {'Pair':<12} {'ML Regime':<12} {'Conf%':>6}  {'Hurst':>6} {'ADX':>6}  {'ML':>4} {'H>.50':>5} {'H>.52':>5} {'H>.55':>5} {'Kombi':>5}")
    print(f"  {'-'*12} {'-'*12} {'-'*6}  {'-'*6} {'-'*6}  {'-'*4} {'-'*5} {'-'*5} {'-'*5} {'-'*5}")

    ml_pass = 0
    h50_pass = 0
    h52_pass = 0
    h55_pass = 0
    kombi_pass = 0
    adx_pass_count = 0

    results = []

    for pair in pairs:
        try:
            df = await pipeline.fetch_candles(pair, "H1", count=200)
            df = pipeline.compute_features(df, pair)
            await asyncio.sleep(0.3)  # Rate limit

            hurst = float(df["hurst_exponent"].iloc[-1]) if "hurst_exponent" in df.columns else float("nan")
            adx_val = float(df["adx"].iloc[-1]) if "adx" in df.columns else float("nan")

            # ML regime prediction
            ml_regime = "no model"
            ml_conf = 0.0
            if pair in detectors:
                ml_regime, ml_conf = detectors[pair].predict(df)

            # Filter results
            ml_ok = not (ml_regime == "ranging" and ml_conf >= 0.70)
            h50_ok = hurst >= 0.50
            h52_ok = hurst >= 0.52
            h55_ok = hurst >= 0.55
            adx_ok = adx_val >= 25
            # Kombi: Hurst >= 0.48 AND (ADX >= 22 OR Hurst >= 0.55)
            kombi_ok = hurst >= 0.48 and (adx_val >= 22 or hurst >= 0.55)

            if ml_ok: ml_pass += 1
            if h50_ok: h50_pass += 1
            if h52_ok: h52_pass += 1
            if h55_ok: h55_pass += 1
            if kombi_ok: kombi_pass += 1
            if adx_ok: adx_pass_count += 1

            tick = lambda ok: " \033[32mOK\033[0m" if ok else " \033[31m--\033[0m"

            print(
                f"  {pair:<12} {ml_regime:<12} {ml_conf*100:5.1f}%  "
                f"{hurst:6.3f} {adx_val:6.1f}  "
                f"{tick(ml_ok)} {tick(h50_ok)} {tick(h52_ok)} {tick(h55_ok)} {tick(kombi_ok)}"
            )

            results.append({
                "pair": pair, "ml_regime": ml_regime, "ml_conf": ml_conf,
                "hurst": hurst, "adx": adx_val, "ml_ok": ml_ok,
                "h50_ok": h50_ok, "h52_ok": h52_ok, "h55_ok": h55_ok,
                "kombi_ok": kombi_ok, "adx_ok": adx_ok,
            })

        except Exception as e:
            print(f"  {pair:<12} ERROR: {e}")

    # Summary
    n = len(results)
    print(f"\n  {'='*90}")
    print(f"  ZUSAMMENFASSUNG ({n} Paare)")
    print(f"  {'='*90}")
    print(f"  ML Regime (>=70% conf):     {ml_pass:>2}/{n} Paare passen durch")
    print(f"  Hurst > 0.50:               {h50_pass:>2}/{n} Paare passen durch")
    print(f"  Hurst > 0.52:               {h52_pass:>2}/{n} Paare passen durch")
    print(f"  Hurst > 0.55:               {h55_pass:>2}/{n} Paare passen durch")
    print(f"  Kombi (H>=.48 & ADX>=22):   {kombi_pass:>2}/{n} Paare passen durch")
    print(f"  (ADX >= 25 allein:          {adx_pass_count:>2}/{n})")

    # Agreement analysis
    if results:
        agree = sum(1 for r in results if r["ml_ok"] == r["h52_ok"])
        disagree_ml_strict = sum(1 for r in results if r["ml_ok"] and not r["h52_ok"])
        disagree_h_strict = sum(1 for r in results if not r["ml_ok"] and r["h52_ok"])
        print(f"\n  ML vs Hurst>0.52 Übereinstimmung: {agree}/{n} ({agree/n*100:.0f}%)")
        print(f"    ML lässt durch, Hurst blockt:   {disagree_ml_strict}")
        print(f"    Hurst lässt durch, ML blockt:   {disagree_h_strict}")

        agree_k = sum(1 for r in results if r["ml_ok"] == r["kombi_ok"])
        print(f"\n  ML vs Kombi Übereinstimmung:      {agree_k}/{n} ({agree_k/n*100:.0f}%)")

    await broker.close()


if __name__ == "__main__":
    asyncio.run(main())
