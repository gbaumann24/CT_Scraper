import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Datei einlesen
file_path = "omr_project-management_reviews.csv"  # Passe den Dateinamen an
df = pd.read_csv(file_path)

# ✅ Zeitzonen korrekt setzen und Warnung vermeiden
df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)

# ✅ Filterung anpassen (Vergleich mit Timestamp)
df = df[(df["published_at"] >= pd.Timestamp("2023-01-01", tz="UTC")) & (df["recommendation_score"] >= 7)]

# ✅ Gruppieren: Welche Tools in welchen Branchen genutzt werden?
pivot_table = df.groupby(["company_field", "product"]).size().reset_index(name="count")

# ✅ Heatmap vorbereiten (Pivot für Visualisierung)
heatmap_data = pivot_table.pivot_table(index="company_field", columns="product", values="count", fill_value=0)

# ✅ **Normierung pro Branche (`company_field`)**
heatmap_data = heatmap_data.div(heatmap_data.sum(axis=1), axis=0)  # Jede Zeile durch ihre Summe teilen
heatmap_data = heatmap_data.loc[(heatmap_data.sum(axis=1) > 0)]  # Nur Branchen mit mindestens einem Tool
heatmap_data = heatmap_data.loc[:, (heatmap_data.sum(axis=0) > 0)]  # Nur Tools mit mindestens einer Branche

# 🔥 Plot erstellen
plt.figure(figsize=(15, 8))
sns.heatmap(heatmap_data, cmap="Blues", linewidths=1, annot=False, fmt=".2f", xticklabels=True, yticklabels=True)

plt.title("Relative Nutzung von Tools in verschiedenen Branchen (Score 7+ ab 2023)")
plt.xlabel("Produkt", fontsize=6)
plt.ylabel("Branche", fontsize=6)
plt.xticks(rotation=90, fontsize=6)
plt.yticks(rotation=0, fontsize=4)
plt.yticks(rotation=0, ha="right", fontsize=6) 


print(heatmap_data)

plt.show()


