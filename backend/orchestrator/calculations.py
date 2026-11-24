def calculate(job):
    """Строки синтетического результата вычисляются из закреплённых входов."""
    feed = job["inputs"]
    result = []
    for i in range(8):
        values = {k: v["rows"][i] for k, v in feed.items()}
        if job["kind"] == "P1":
            value = values["deals"] * values["rates"] + values["reference"]
        elif job["kind"] == "P2":
            value = abs(values["deals"] - values["rates"]) * values["reference"]
        else:
            value = sum(values.values()) + i + 1
        result.append(value * job["factor"])
    return result
