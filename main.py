import json
import csv
import argparse
from pathlib import Path
from typing import Dict, List, Set
from collections import defaultdict
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PharmacyDataProcessor:
    """Processes pharmacy claims and reverts data to calculate metrics."""

    def __init__(self):
        self.pharmacies: Dict[str, str] = {}
        self.valid_npis: Set[str] = set()
        self.claims: List[Dict] = []
        self.reverts: List[Dict] = []

    def load_pharmacies(self, pharmacy_dir: str) -> None:
        """Load pharmacy data from CSV files in a single directory."""
        logger.info(f"Loading pharmacies from {pharmacy_dir}")

        pharmacy_path = Path(pharmacy_dir)
        if not pharmacy_path.exists():
            raise FileNotFoundError(f"Pharmacy directory not found: {pharmacy_dir}")

        # Process all CSV files in the directory
        for csv_file in pharmacy_path.glob("*.csv"):
            logger.info(f"Processing pharmacy file: {csv_file}")
            try:
                with open(csv_file, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if "npi" in row and "chain" in row:
                            npi = row["npi"].strip()
                            chain = row["chain"].strip()
                            if npi and chain:
                                self.pharmacies[npi] = chain
                                self.valid_npis.add(npi)
            except Exception as e:
                logger.error(f"Error reading pharmacy file {csv_file}: {e}")

        logger.info(f"Loaded {len(self.pharmacies)} pharmacies")

    def load_claims(self, claims_dir: str) -> None:
        """Load claims data from JSON files in a single directory."""
        logger.info(f"Loading claims from {claims_dir}")

        claims_path = Path(claims_dir)
        if not claims_path.exists():
            raise FileNotFoundError(f"Claims directory not found: {claims_dir}")

        # Process all JSON files in the directory
        for json_file in claims_path.glob("*.json"):
            logger.info(f"Processing claims file: {json_file}")
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                if isinstance(data, list):
                    claims_batch = data
                else:
                    claims_batch = [data]

                for claim in claims_batch:
                    if self._validate_claim(claim):
                        if claim["npi"] in self.valid_npis:
                            self.claims.append(claim)

            except Exception as e:
                logger.error(f"Error reading claims file {json_file}: {e}")

        logger.info(f"Loaded {len(self.claims)} valid claims")

    def load_reverts(self, reverts_dir: str) -> None:
        """Load reverts data from JSON files in a single directory."""
        logger.info(f"Loading reverts from {reverts_dir}")

        reverts_path = Path(reverts_dir)
        if not reverts_path.exists():
            raise FileNotFoundError(f"Reverts directory not found: {reverts_dir}")

        for json_file in reverts_path.glob("*.json"):
            logger.info(f"Processing reverts file: {json_file}")
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                if isinstance(data, list):
                    reverts_batch = data
                else:
                    reverts_batch = [data]

                for revert in reverts_batch:
                    if self._validate_revert(revert):
                        self.reverts.append(revert)

            except Exception as e:
                logger.error(f"Error reading reverts file {json_file}: {e}")

        logger.info(f"Loaded {len(self.reverts)} valid reverts")

    def _validate_claim(self, claim: Dict) -> bool:
        """Validate claim schema."""
        required_fields = ["id", "npi", "ndc", "price", "quantity", "timestamp"]

        for field in required_fields:
            if field not in claim:
                return False

        try:
            float(claim["price"])
            int(claim["quantity"])
            return True
        except (ValueError, TypeError):
            return False

    def _validate_revert(self, revert: Dict) -> bool:
        """Validate revert schema."""
        required_fields = ["id", "claim_id", "timestamp"]

        for field in required_fields:
            if field not in revert:
                return False

    def calculate_metrics(self) -> List[Dict]:
        """Calculate metrics grouped by npi and ndc."""
        logger.info("Calculating metrics...")

        # Create a set of reverted claim IDs for quick lookup
        reverted_claim_ids = {revert["claim_id"] for revert in self.reverts}

        # Group data by (npi, ndc)
        metrics = defaultdict(
            lambda: {"fills": 0, "reverted": 0, "total_price": 0.0, "prices": []}
        )

        # Process claims
        for claim in self.claims:
            npi = claim["npi"]
            ndc = claim["ndc"]
            key = (npi, ndc)

            price = float(claim["price"])
            quantity = int(claim["quantity"])

            metrics[key]["fills"] += 1
            metrics[key]["total_price"] += price

            # Calculate unit price for average
            unit_price = price / quantity if quantity > 0 else 0
            metrics[key]["prices"].append(unit_price)

            # Check if this claim is reverted
            if claim["id"] in reverted_claim_ids:
                metrics[key]["reverted"] += 1

        # Convert to output format
        results = []
        for (npi, ndc), data in metrics.items():
            avg_price = (
                sum(data["prices"]) / len(data["prices"]) if data["prices"] else 0.0
            )

            results.append(
                {
                    "npi": npi,
                    "ndc": ndc,
                    "fills": data["fills"],
                    "reverted": data["reverted"],
                    "avg_price": round(avg_price, 2),
                    "total_price": round(data["total_price"], 2),
                }
            )

        # Sort by npi, then ndc for consistent output
        results.sort(key=lambda x: (x["npi"], x["ndc"]))

        logger.info(f"Calculated metrics for {len(results)} npi/ndc combinations")
        return results

    def save_results(self, results: List[Dict], output_file: str) -> None:
        """Save results to JSON file."""
        logger.info(f"Saving results to {output_file}")

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        logger.info(f"Results saved successfully to {output_file}")

    def analyze_chain_recommendations(self) -> List[Dict]:
        """Item 3: Top 2 chains by drug with lowest average unit prices."""
        logger.info("Analyzing chain recommendations...")

        # Create DataFrame from claims data
        claims_data = []
        for claim in self.claims:
            npi = claim["npi"]
            if npi in self.pharmacies:
                claims_data.append(
                    {
                        "ndc": claim["ndc"],
                        "npi": npi,
                        "chain": self.pharmacies[npi],
                        "price": float(claim["price"]),
                        "quantity": int(claim["quantity"]),
                    }
                )

        if not claims_data:
            logger.warning("No valid claims data for chain analysis")
            return []

        df = pd.DataFrame(claims_data)

        df["unit_price"] = df["price"] / df["quantity"]

        chain_avg = df.groupby(["ndc", "chain"])["unit_price"].mean().reset_index()
        chain_avg.columns = ["ndc", "chain", "avg_price"]

        results = []
        for ndc in chain_avg["ndc"].unique():
            ndc_data = (
                chain_avg[chain_avg["ndc"] == ndc].sort_values("avg_price").head(2)
            )

            chain_list = []
            for _, row in ndc_data.iterrows():
                chain_list.append(
                    {"name": row["chain"], "avg_price": round(row["avg_price"], 2)}
                )

            results.append({"ndc": ndc, "chain": chain_list})

        results.sort(key=lambda x: x["ndc"])

        logger.info(f"Generated chain recommendations for {len(results)} drugs")
        return results

    def analyze_common_quantities(self) -> List[Dict]:
        """Item 4: Most common quantities prescribed per drug."""
        logger.info("Analyzing common quantities...")

        # Create DataFrame from claims data
        claims_data = []
        for claim in self.claims:
            if claim["npi"] in self.pharmacies:
                claims_data.append(
                    {
                        "ndc": claim["ndc"],
                        "quantity": float(claim["quantity"]),
                    }
                )

        if not claims_data:
            logger.warning("No valid claims data for quantity analysis")
            return []

        df = pd.DataFrame(claims_data)

        results = []
        for ndc in df["ndc"].unique():
            ndc_data = df[df["ndc"] == ndc]

            # Get the most common quantities (top 5)
            quantity_counts = ndc_data["quantity"].value_counts().head(5)
            most_prescribed = quantity_counts.index.tolist()

            results.append({"ndc": ndc, "most_prescribed_quantity": most_prescribed})

        results.sort(key=lambda x: x["ndc"])

        logger.info(f"Generated quantity analysis for {len(results)} drugs")
        return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Process pharmacy claims data")
    parser.add_argument(
        "--pharmacy-dir",
        required=True,
        help="Directory containing pharmacy CSV files",
    )
    parser.add_argument(
        "--claims-dir",
        required=True,
        help="Directory containing claims JSON files",
    )
    parser.add_argument(
        "--reverts-dir",
        required=True,
        help="Directory containing reverts JSON files",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="metrics_output.json",
        help="Output JSON file (default: metrics_output.json)",
    )

    args = parser.parse_args()

    # Initialize processor
    processor = PharmacyDataProcessor()

    try:
        # Load data
        # processor.load_pharmacies("data/pharmacies")
        # processor.load_claims("data/claims")
        # processor.load_reverts("data/reverts")
        processor.load_pharmacies(args.pharmacy_dir)
        processor.load_claims(args.claims_dir)
        processor.load_reverts(args.reverts_dir)

        # Calculate metrics (Items 1 & 2)
        results = processor.calculate_metrics()
        processor.save_results(results, "metrics_output.json")

        # Analyze chain recommendations (Item 3)
        chain_recommendations = processor.analyze_chain_recommendations()
        processor.save_results(chain_recommendations, "chain_recommendations.json")

        # Analyze common quantities (Item 4)
        quantity_analysis = processor.analyze_common_quantities()
        processor.save_results(quantity_analysis, "quantity_analysis.json")

        logger.info("Processing completed successfully!")
        logger.info(f"Items 1-2: {len(results)} npi/ndc combinations processed")
        logger.info(
            f"Item 3: {len(chain_recommendations)} drugs analyzed for chain recommendations"
        )
        logger.info(
            f"Item 4: {len(quantity_analysis)} drugs analyzed for quantity patterns"
        )

    except Exception as e:
        logger.error(f"Error during processing: {e}")
        raise


if __name__ == "__main__":
    main()
