"""Tests for Fronius powerflow parsing."""

import unittest

from batcontrol.inverter.fronius import FroniusWR


class TestFroniusPowerflow(unittest.TestCase):
    """Test parsing of actual powerflow metrics from Solar API JSON."""

    def test_get_powerflow_metrics_parses_site_and_secondary_meter_values(self):
        inverter = FroniusWR.__new__(FroniusWR)
        inverter._soc_cache = {}
        inverter.inverter_id = '1'
        inverter._get_powerflow_result = lambda: {
            'Body': {
                'Data': {
                    'Inverters': {
                        '1': {
                            'P': 643.85516357421875,
                            'SOC': 69.0,
                        }
                    },
                    'SecondaryMeters': {
                        '1': {
                            'Category': 'METER_CAT_WR',
                            'P': 562.1,
                        }
                    },
                    'Site': {
                        'P_Akku': -3388.453857421875,
                        'P_Grid': 29.2,
                        'P_Load': -1235.3535888671875,
                        'P_PV': 4035.735107421875,
                    },
                }
            }
        }

        metrics = FroniusWR.get_powerflow_metrics(inverter)

        self.assertAlmostEqual(metrics['actual_pv_w'], 4035.735107421875)
        self.assertAlmostEqual(metrics['actual_secondary_wr_w'], 562.1)
        self.assertAlmostEqual(metrics['actual_production_w'], 4597.835107421875)
        self.assertAlmostEqual(metrics['actual_inverter_w'], 643.85516357421875)
        self.assertAlmostEqual(metrics['actual_battery_w'], -3388.453857421875)
        self.assertAlmostEqual(metrics['actual_consumption_w'], 1235.3535888671875)
        self.assertAlmostEqual(metrics['actual_grid_w'], 29.2)


if __name__ == '__main__':
    unittest.main()
