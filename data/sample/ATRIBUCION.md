# Atribución de datos

`apu_fleet_sample.jsonl` es un derivado recortado (primeros 5 minutos de tiempo de evento por activo sintetizado) del dataset:

Davari, N., Veloso, B., Ribeiro, R., & Gama, J. (2021). **MetroPT-3 Dataset** [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5VW3R

Licencia: **CC BY 4.0** (Creative Commons Attribution 4.0 International). https://creativecommons.org/licenses/by/4.0/legalcode

La flota de 6 `asset_id` (`apu-01`…`apu-06`) es una **síntesis**: tramos temporales reales y disjuntos de un único compresor de tren (Air Production Unit), mapeados a activos distintos para ejercitar clave de particionamiento, paralelismo y skew en el pipeline. No representan seis unidades físicas distintas. Ver `docs/apu-streaming.md` sección "Síntesis de flota".
