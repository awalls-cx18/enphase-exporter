import importlib.metadata
import json
import logging
import time
from time import perf_counter as time_now

import prometheus_client
import requests
import tenacity
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

logger = logging.getLogger(__name__)


def get_version(package_name: str = __name__) -> str:
    try:
        return importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        return "0.0.0"


class APIClient:
    def __init__(self: "APIClient", api_url: str) -> None:
        self.api_url = api_url
        self.session = requests.session()
        self.session.headers.update({"User-Agent": ""})

    @tenacity.retry(
        stop=(tenacity.stop_after_delay(30)),
        wait=tenacity.wait_random(min=1, max=2),
        before_sleep=tenacity.before_sleep_log(logger, logging.ERROR),
        reraise=True,
    )
    def call(self: "APIClient", path: str) -> dict:
        api_url = self.api_url.rstrip("/")
        path = path.lstrip("/")
        result = self.session.get(f"{api_url}/{path}")
        result.raise_for_status()

        try:
            return result.json()
        except json.JSONDecodeError:
            logger.error("received invalid data from API:")
            logger.error(result.content)
            raise


class CustomCollector:
    def __init__(self: "CustomCollector", api_url: str) -> None:
        self.client = APIClient(api_url)

    def collect(self: "CustomCollector") -> None:
        timer = time_now()
        data = {
            "production": self.client.call("/production.json"),
            "inverters": self.client.call("/api/v1/production/inverters"),
        }
        logger.info(
            "successfully polled the enphase envoy device in {:.4f} seconds".format(
                time_now() - timer,
            ),
        )

        metrics = {
            "apprnt_pwr": GaugeMetricFamily(
                "solar_flow_apprnt_pwr",
                "Apparent power",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "pwr_factor": GaugeMetricFamily(
                "solar_flow_pwr_factor",
                "Power factor",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "react_pwr": GaugeMetricFamily(
                "solar_flow_react_pwr",
                "Reactive power",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "vah_today": GaugeMetricFamily(
                "solar_flow_vah_today",
                "Volt-amp-hours today",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "vah_lifetime": CounterMetricFamily(
                "solar_flow_vah_lifetime",
                "Volt-amp-hours lifetime",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "varh_lag_today": GaugeMetricFamily(
                "solar_flow_varh_lag_today",
                "Volt-amp-reactive-hours lag today",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "varh_lag_lifetime": CounterMetricFamily(
                "solar_flow_varh_lag_lifetime",
                "Volt-amp-reactive-hours lag lifetime",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "varh_lead_today": GaugeMetricFamily(
                "solar_flow_varh_lead_today",
                "Volt-amp-reactive-hours lead today",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "varh_lead_lifetime": CounterMetricFamily(
                "solar_flow_varh_lead_lifetime",
                "Volt-amp-reactive-hours lead lifetime",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "w_now": GaugeMetricFamily(
                "solar_flow_w_now",
                "Current watts",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "wh_today": GaugeMetricFamily(
                "solar_flow_wh_today",
                "Watt-hours today",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "wh_last7days": GaugeMetricFamily(
                "solar_flow_wh_last7days",
                "Watt-hours last seven days",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "wh_lifetime": CounterMetricFamily(
                "solar_flow_wh_lifetime",
                "Watt-hours lifetime",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "active_count": GaugeMetricFamily(
                "solar_flow_active_count",
                "Active device count",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "reading_time": GaugeMetricFamily(
                "solar_flow_reading_time",
                "Time of the measurement (referenced to the Unix epoch)",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "rms_current": GaugeMetricFamily(
                "solar_flow_rms_current",
                "RMS current",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "rms_voltage": GaugeMetricFamily(
                "solar_flow_rms_voltage",
                "RMS voltage",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "wh_now": GaugeMetricFamily(
                "solar_flow_wh_now",
                "Current watt-hours (battery energy)",
                labels=["direction", "device_type", "measurement_type"],
            ),
            "last_report_watts": GaugeMetricFamily(
                "solar_inverter_last_report_watts",
                "Watts reported by the panel inverter",
                labels=["serial_num", "dev_type"],
            ),
            "max_report_watts": GaugeMetricFamily(
                "solar_inverter_max_report_watts",
                "Max watts ever seen by the panel inverter",
                labels=["serial_num", "dev_type"],
            ),
            "last_report_time": GaugeMetricFamily(
                "solar_inverter_last_report_time",
                "Time of last report",
                labels=["serial_num", "dev_type"],
            ),
        }

        # Inverters, individual power production reports
        for inverter in data["inverters"]:
            labels = [inverter["serialNumber"], str(inverter["devType"])]
            metrics["last_report_watts"].add_metric(
                labels,
                inverter["lastReportWatts"],
            )
            metrics["max_report_watts"].add_metric(
                labels,
                inverter["maxReportWatts"],
            )
            metrics["last_report_time"].add_metric(
                labels,
                inverter["lastReportDate"],
            )

        # System power flows

        # Production power flow
        for datum in data["production"]["production"]:

            # Inverter reported production, aggregate
            if datum.get("type", "") == "inverters":
                labels = ["production", datum["type"], "inverters_production"]
                metrics["active_count"].add_metric(labels, datum["activeCount"])
                metrics["reading_time"].add_metric(labels, datum["readingTime"])
                metrics["w_now"].add_metric(labels, datum["wNow"])
                metrics["wh_lifetime"].add_metric(labels, datum["whLifetime"])

            # TODO: handle the following meter types:
            # Revenue Grade Meter: type == "rgms"
            # Power Meter Unit: type == "pmus"

            # Envoy Integrated Meter measured production
            if datum.get("type", "") == "eim":
                labels = ["production", datum["type"], datum["measurementType"]]
                metrics["apprnt_pwr"].add_metric(labels, datum["apprntPwr"])
                metrics["pwr_factor"].add_metric(labels, datum["pwrFactor"])
                metrics["react_pwr"].add_metric(labels, datum["reactPwr"])
                metrics["vah_today"].add_metric(labels, datum["vahToday"])
                metrics["vah_lifetime"].add_metric(labels, datum["vahLifetime"])
                metrics["varh_lag_today"].add_metric(
                    labels,
                    datum["varhLagToday"],
                )
                metrics["varh_lag_lifetime"].add_metric(
                    labels,
                    datum["varhLagLifetime"],
                )
                metrics["varh_lead_today"].add_metric(
                    labels,
                    datum["varhLeadToday"],
                )
                metrics["varh_lead_lifetime"].add_metric(
                    labels,
                    datum["varhLeadLifetime"],
                )
                metrics["w_now"].add_metric(labels, datum["wNow"])
                metrics["wh_today"].add_metric(labels, datum["whToday"])
                metrics["wh_last7days"].add_metric(
                    labels,
                    datum["whLastSevenDays"],
                )
                metrics["wh_lifetime"].add_metric(labels, datum["whLifetime"])
                metrics["active_count"].add_metric(labels, datum["activeCount"])
                metrics["reading_time"].add_metric(labels, datum["readingTime"])
                metrics["rms_current"].add_metric(labels, datum["rmsCurrent"])
                metrics["rms_voltage"].add_metric(labels, datum["rmsVoltage"])

        # Consumption power flow
        for datum in data["production"]["consumption"]:

            # TODO: handle the following meter types:
            # Revenue Grade Meter: type == "rgms"
            # Power Meter Unit: type == "pmus"

            # Envoy Integrated Meter measured consumption
            if datum.get("type", "") == "eim":
                labels = ["consumption", datum["type"], datum["measurementType"]]
                metrics["apprnt_pwr"].add_metric(labels, datum["apprntPwr"])
                metrics["pwr_factor"].add_metric(labels, datum["pwrFactor"])
                metrics["react_pwr"].add_metric(labels, datum["reactPwr"])
                metrics["vah_today"].add_metric(labels, datum["vahToday"])
                metrics["vah_lifetime"].add_metric(labels, datum["vahLifetime"])
                metrics["varh_lag_today"].add_metric(
                    labels,
                    datum["varhLagToday"],
                )
                metrics["varh_lag_lifetime"].add_metric(
                    labels,
                    datum["varhLagLifetime"],
                )
                metrics["varh_lead_today"].add_metric(
                    labels,
                    datum["varhLeadToday"],
                )
                metrics["varh_lead_lifetime"].add_metric(
                    labels,
                    datum["varhLeadLifetime"],
                )
                metrics["w_now"].add_metric(labels, datum["wNow"])
                metrics["wh_today"].add_metric(labels, datum["whToday"])
                metrics["wh_last7days"].add_metric(
                    labels,
                    datum["whLastSevenDays"],
                )
                metrics["wh_lifetime"].add_metric(labels, datum["whLifetime"])
                metrics["active_count"].add_metric(labels, datum["activeCount"])
                metrics["reading_time"].add_metric(labels, datum["readingTime"])
                metrics["rms_current"].add_metric(labels, datum["rmsCurrent"])
                metrics["rms_voltage"].add_metric(labels, datum["rmsVoltage"])

        # Storage power flow
        for datum in data["production"]["storage"]:

            # AC Battery
            if datum.get("type", "") == "acb":
                labels = ["storage", datum["type"], "batteries"]
                metrics["active_count"].add_metric(labels, datum["activeCount"])
                metrics["reading_time"].add_metric(labels, datum["readingTime"])
                metrics["w_now"].add_metric(labels, datum["wNow"])
                metrics["wh_now"].add_metric(labels, datum["whNow"])
                # TODO Translate "state" values into a metric
                # (full, charging, idle, discharging)

        yield from metrics.values()


def run(
    port: int,
    api_url: str,
) -> None:
    logger.info(f"starting exporter on port {port} connecting to {api_url}")
    prometheus_client.start_http_server(port)

    # disable metrics that we do not care about
    prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)
    prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)

    # enable our custom metric collector
    prometheus_client.REGISTRY.register(CustomCollector(api_url))

    while True:
        time.sleep(10)
