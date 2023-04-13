package bindgpu

import (
	"context"
	"encoding/xml"
	"fmt"
	"testing"
)

func TestFormatGpu(t *testing.T) {
	input := []byte(`
<?xml version="1.0" ?>
<!DOCTYPE nvidia_smi_log SYSTEM "nvsmi_device_v10.dtd">
<nvidia_smi_log>
        <timestamp>Thu Dec 10 19:57:44 2020</timestamp>
        <driver_version>440.82</driver_version>
        <cuda_version>10.2</cuda_version>
        <attached_gpus>2</attached_gpus>
        <gpu id="00000000:3B:00.0">
                <product_name>GeForce RTX 2080 Ti</product_name>
                <product_brand>GeForce</product_brand>
                <display_mode>Disabled</display_mode>
                <display_active>Disabled</display_active>
                <persistence_mode>Disabled</persistence_mode>
                <accounting_mode>Disabled</accounting_mode>
                <accounting_mode_buffer_size>4000</accounting_mode_buffer_size>
                <driver_model>
                        <current_dm>N/A</current_dm>
                        <pending_dm>N/A</pending_dm>
                </driver_model>
                <serial>N/A</serial>
                <uuid>GPU-63716cc6-d3aa-a579-f013-9c5fdc4c7bcf</uuid>
                <minor_number>0</minor_number>
                <vbios_version>90.02.42.00.5A</vbios_version>
                <multigpu_board>No</multigpu_board>
                <board_id>0x3b00</board_id>
                <gpu_part_number>N/A</gpu_part_number>
                <inforom_version>
                        <img_version>G001.0000.02.04</img_version>
                        <oem_object>1.1</oem_object>
                        <ecc_object>N/A</ecc_object>
                        <pwr_object>N/A</pwr_object>
                </inforom_version>
                <gpu_operation_mode>
                        <current_gom>N/A</current_gom>
                        <pending_gom>N/A</pending_gom>
                </gpu_operation_mode>
                <gpu_virtualization_mode>
                        <virtualization_mode>None</virtualization_mode>
                        <host_vgpu_mode>N/A</host_vgpu_mode>
                </gpu_virtualization_mode>
                <ibmnpu>
                        <relaxed_ordering_mode>N/A</relaxed_ordering_mode>
                </ibmnpu>
                <pci>
                        <pci_bus>3B</pci_bus>
                        <pci_device>00</pci_device>
                        <pci_domain>0000</pci_domain>
                        <pci_device_id>1E0710DE</pci_device_id>
                        <pci_bus_id>00000000:3B:00.0</pci_bus_id>
                        <pci_sub_system_id>150319DA</pci_sub_system_id>
                        <pci_gpu_link_info>
                                <pcie_gen>
                                        <max_link_gen>3</max_link_gen>
                                        <current_link_gen>1</current_link_gen>
                                </pcie_gen>
                                <link_widths>
                                        <max_link_width>16x</max_link_width>
                                        <current_link_width>16x</current_link_width>
                                </link_widths>
                        </pci_gpu_link_info>
                        <pci_bridge_chip>
                                <bridge_chip_type>N/A</bridge_chip_type>
                                <bridge_chip_fw>N/A</bridge_chip_fw>
                        </pci_bridge_chip>
                        <replay_counter>0</replay_counter>
                        <replay_rollover_counter>0</replay_rollover_counter>
                        <tx_util>0 KB/s</tx_util>
                        <rx_util>0 KB/s</rx_util>
                </pci>
                <fan_speed>28 %</fan_speed>
                <performance_state>P8</performance_state>
                <clocks_throttle_reasons>
                        <clocks_throttle_reason_gpu_idle>Active</clocks_throttle_reason_gpu_idle>
                        <clocks_throttle_reason_applications_clocks_setting>Not Active</clocks_throttle_reason_applications_clocks_setting>
                        <clocks_throttle_reason_sw_power_cap>Not Active</clocks_throttle_reason_sw_power_cap>
                        <clocks_throttle_reason_hw_slowdown>Not Active</clocks_throttle_reason_hw_slowdown>
                        <clocks_throttle_reason_hw_thermal_slowdown>Not Active</clocks_throttle_reason_hw_thermal_slowdown>
                        <clocks_throttle_reason_hw_power_brake_slowdown>Not Active</clocks_throttle_reason_hw_power_brake_slowdown>
                        <clocks_throttle_reason_sync_boost>Not Active</clocks_throttle_reason_sync_boost>
                        <clocks_throttle_reason_sw_thermal_slowdown>Not Active</clocks_throttle_reason_sw_thermal_slowdown>
                        <clocks_throttle_reason_display_clocks_setting>Not Active</clocks_throttle_reason_display_clocks_setting>
                </clocks_throttle_reasons>
                <fb_memory_usage>
                        <total>11019 MiB</total>
                        <used>0 MiB</used>
                        <free>11019 MiB</free>
                </fb_memory_usage>
                <bar1_memory_usage>
                        <total>256 MiB</total>
                        <used>2 MiB</used>
                        <free>254 MiB</free>
                </bar1_memory_usage>
                <compute_mode>Default</compute_mode>
                <utilization>
                        <gpu_util>0 %</gpu_util>
                        <memory_util>0 %</memory_util>
                        <encoder_util>0 %</encoder_util>
                        <decoder_util>0 %</decoder_util>
                </utilization>
                <encoder_stats>
                        <session_count>0</session_count>
                        <average_fps>0</average_fps>
                        <average_latency>0</average_latency>
                </encoder_stats>
                <fbc_stats>
                        <session_count>0</session_count>
                        <average_fps>0</average_fps>
                        <average_latency>0</average_latency>
                </fbc_stats>
                <ecc_mode>
                        <current_ecc>N/A</current_ecc>
                        <pending_ecc>N/A</pending_ecc>
                </ecc_mode>
                <ecc_errors>
                        <volatile>
                                <sram_correctable>N/A</sram_correctable>
                                <sram_uncorrectable>N/A</sram_uncorrectable>
                                <dram_correctable>N/A</dram_correctable>
                                <dram_uncorrectable>N/A</dram_uncorrectable>
                        </volatile>
                        <aggregate>
                                <sram_correctable>N/A</sram_correctable>
                                <sram_uncorrectable>N/A</sram_uncorrectable>
                                <dram_correctable>N/A</dram_correctable>
                                <dram_uncorrectable>N/A</dram_uncorrectable>
                        </aggregate>
                </ecc_errors>
                <retired_pages>
                        <multiple_single_bit_retirement>
                                <retired_count>N/A</retired_count>
                                <retired_pagelist>N/A</retired_pagelist>
                        </multiple_single_bit_retirement>
                        <double_bit_retirement>
                                <retired_count>N/A</retired_count>
                                <retired_pagelist>N/A</retired_pagelist>
                        </double_bit_retirement>
                        <pending_blacklist>N/A</pending_blacklist>
                        <pending_retirement>N/A</pending_retirement>
                </retired_pages>
                <temperature>
                        <gpu_temp>30 C</gpu_temp>
                        <gpu_temp_max_threshold>94 C</gpu_temp_max_threshold>
                        <gpu_temp_slow_threshold>91 C</gpu_temp_slow_threshold>
                        <gpu_temp_max_gpu_threshold>89 C</gpu_temp_max_gpu_threshold>
                        <memory_temp>N/A</memory_temp>
                        <gpu_temp_max_mem_threshold>N/A</gpu_temp_max_mem_threshold>
                </temperature>
                <power_readings>
                        <power_state>P8</power_state>
                        <power_management>Supported</power_management>
                        <power_draw>7.17 W</power_draw>
                        <power_limit>257.00 W</power_limit>
                        <default_power_limit>257.00 W</default_power_limit>
                        <enforced_power_limit>257.00 W</enforced_power_limit>
                        <min_power_limit>100.00 W</min_power_limit>
                        <max_power_limit>280.00 W</max_power_limit>
                </power_readings>
                <clocks>
                        <graphics_clock>300 MHz</graphics_clock>
                        <sm_clock>300 MHz</sm_clock>
                        <mem_clock>405 MHz</mem_clock>
                        <video_clock>540 MHz</video_clock>
                </clocks>
                <applications_clocks>
                        <graphics_clock>N/A</graphics_clock>
                        <mem_clock>N/A</mem_clock>
                </applications_clocks>
                <default_applications_clocks>
                        <graphics_clock>N/A</graphics_clock>
                        <mem_clock>N/A</mem_clock>
                </default_applications_clocks>
                <max_clocks>
                        <graphics_clock>2115 MHz</graphics_clock>
                        <sm_clock>2115 MHz</sm_clock>
                        <mem_clock>7000 MHz</mem_clock>
                        <video_clock>1950 MHz</video_clock>
                </max_clocks>
                <max_customer_boost_clocks>
                        <graphics_clock>N/A</graphics_clock>
                </max_customer_boost_clocks>
                <clock_policy>
                        <auto_boost>N/A</auto_boost>
                        <auto_boost_default>N/A</auto_boost_default>
                </clock_policy>
                <supported_clocks>N/A</supported_clocks>
                <processes>
                </processes>
                <accounted_processes>
                </accounted_processes>
        </gpu>

        <gpu id="00000000:D8:00.0">
                <product_name>GeForce RTX 2080 Ti</product_name>
                <product_brand>GeForce</product_brand>
                <display_mode>Disabled</display_mode>
                <display_active>Disabled</display_active>
                <persistence_mode>Disabled</persistence_mode>
                <accounting_mode>Disabled</accounting_mode>
                <accounting_mode_buffer_size>4000</accounting_mode_buffer_size>
                <driver_model>
                        <current_dm>N/A</current_dm>
                        <pending_dm>N/A</pending_dm>
                </driver_model>
                <serial>N/A</serial>
                <uuid>GPU-6750d85a-53a7-8bd5-a687-bd27c447a35d</uuid>
                <minor_number>1</minor_number>
                <vbios_version>90.02.42.00.5A</vbios_version>
                <multigpu_board>No</multigpu_board>
                <board_id>0xd800</board_id>
                <gpu_part_number>N/A</gpu_part_number>
                <inforom_version>
                        <img_version>G001.0000.02.04</img_version>
                        <oem_object>1.1</oem_object>
                        <ecc_object>N/A</ecc_object>
                        <pwr_object>N/A</pwr_object>
                </inforom_version>
                <gpu_operation_mode>
                        <current_gom>N/A</current_gom>
                        <pending_gom>N/A</pending_gom>
                </gpu_operation_mode>
                <gpu_virtualization_mode>
                        <virtualization_mode>None</virtualization_mode>
                        <host_vgpu_mode>N/A</host_vgpu_mode>
                </gpu_virtualization_mode>
                <ibmnpu>
                        <relaxed_ordering_mode>N/A</relaxed_ordering_mode>
                </ibmnpu>
                <pci>
                        <pci_bus>D8</pci_bus>
                        <pci_device>00</pci_device>
                        <pci_domain>0000</pci_domain>
                        <pci_device_id>1E0710DE</pci_device_id>
                        <pci_bus_id>00000000:D8:00.0</pci_bus_id>
                        <pci_sub_system_id>150319DA</pci_sub_system_id>
                        <pci_gpu_link_info>
                                <pcie_gen>
                                        <max_link_gen>3</max_link_gen>
                                        <current_link_gen>1</current_link_gen>
                                </pcie_gen>
                                <link_widths>
                                        <max_link_width>16x</max_link_width>
                                        <current_link_width>16x</current_link_width>
                                </link_widths>
                        </pci_gpu_link_info>
                        <pci_bridge_chip>
                                <bridge_chip_type>N/A</bridge_chip_type>
                                <bridge_chip_fw>N/A</bridge_chip_fw>
                        </pci_bridge_chip>
                        <replay_counter>0</replay_counter>
                        <replay_rollover_counter>0</replay_rollover_counter>
                        <tx_util>0 KB/s</tx_util>
                        <rx_util>0 KB/s</rx_util>
                </pci>
                <fan_speed>28 %</fan_speed>
                <performance_state>P8</performance_state>
                <clocks_throttle_reasons>
                        <clocks_throttle_reason_gpu_idle>Active</clocks_throttle_reason_gpu_idle>
                        <clocks_throttle_reason_applications_clocks_setting>Not Active</clocks_throttle_reason_applications_clocks_setting>
                        <clocks_throttle_reason_sw_power_cap>Not Active</clocks_throttle_reason_sw_power_cap>
                        <clocks_throttle_reason_hw_slowdown>Not Active</clocks_throttle_reason_hw_slowdown>
                        <clocks_throttle_reason_hw_thermal_slowdown>Not Active</clocks_throttle_reason_hw_thermal_slowdown>
                        <clocks_throttle_reason_hw_power_brake_slowdown>Not Active</clocks_throttle_reason_hw_power_brake_slowdown>
                        <clocks_throttle_reason_sync_boost>Not Active</clocks_throttle_reason_sync_boost>
                        <clocks_throttle_reason_sw_thermal_slowdown>Not Active</clocks_throttle_reason_sw_thermal_slowdown>
                        <clocks_throttle_reason_display_clocks_setting>Not Active</clocks_throttle_reason_display_clocks_setting>
                </clocks_throttle_reasons>
                <fb_memory_usage>
                        <total>11019 MiB</total>
                        <used>0 MiB</used>
                        <free>11019 MiB</free>
                </fb_memory_usage>
                <bar1_memory_usage>
                        <total>256 MiB</total>
                        <used>2 MiB</used>
                        <free>254 MiB</free>
                </bar1_memory_usage>
                <compute_mode>Default</compute_mode>
                <utilization>
                        <gpu_util>0 %</gpu_util>
                        <memory_util>0 %</memory_util>
                        <encoder_util>0 %</encoder_util>
                        <decoder_util>0 %</decoder_util>
                </utilization>
                <encoder_stats>
                        <session_count>0</session_count>
                        <average_fps>0</average_fps>
                        <average_latency>0</average_latency>
                </encoder_stats>
                <fbc_stats>
                        <session_count>0</session_count>
                        <average_fps>0</average_fps>
                        <average_latency>0</average_latency>
                </fbc_stats>
                <ecc_mode>
                        <current_ecc>N/A</current_ecc>
                        <pending_ecc>N/A</pending_ecc>
                </ecc_mode>
                <ecc_errors>
                        <volatile>
                                <sram_correctable>N/A</sram_correctable>
                                <sram_uncorrectable>N/A</sram_uncorrectable>
                                <dram_correctable>N/A</dram_correctable>
                                <dram_uncorrectable>N/A</dram_uncorrectable>
                        </volatile>
                        <aggregate>
                                <sram_correctable>N/A</sram_correctable>
                                <sram_uncorrectable>N/A</sram_uncorrectable>
                                <dram_correctable>N/A</dram_correctable>
                                <dram_uncorrectable>N/A</dram_uncorrectable>
                        </aggregate>
                </ecc_errors>
                <retired_pages>
                        <multiple_single_bit_retirement>
                                <retired_count>N/A</retired_count>
                                <retired_pagelist>N/A</retired_pagelist>
                        </multiple_single_bit_retirement>
                        <double_bit_retirement>
                                <retired_count>N/A</retired_count>
                                <retired_pagelist>N/A</retired_pagelist>
                        </double_bit_retirement>
                        <pending_blacklist>N/A</pending_blacklist>
                        <pending_retirement>N/A</pending_retirement>
                </retired_pages>
                <temperature>
                        <gpu_temp>32 C</gpu_temp>
                        <gpu_temp_max_threshold>94 C</gpu_temp_max_threshold>
                        <gpu_temp_slow_threshold>91 C</gpu_temp_slow_threshold>
                        <gpu_temp_max_gpu_threshold>89 C</gpu_temp_max_gpu_threshold>
                        <memory_temp>N/A</memory_temp>
                        <gpu_temp_max_mem_threshold>N/A</gpu_temp_max_mem_threshold>
                </temperature>
                <power_readings>
                        <power_state>P8</power_state>
                        <power_management>Supported</power_management>
                        <power_draw>4.95 W</power_draw>
                        <power_limit>257.00 W</power_limit>
                        <default_power_limit>257.00 W</default_power_limit>
                        <enforced_power_limit>257.00 W</enforced_power_limit>
                        <min_power_limit>100.00 W</min_power_limit>
                        <max_power_limit>280.00 W</max_power_limit>
                </power_readings>
                <clocks>
                        <graphics_clock>300 MHz</graphics_clock>
                        <sm_clock>300 MHz</sm_clock>
                        <mem_clock>405 MHz</mem_clock>
                        <video_clock>540 MHz</video_clock>
                </clocks>
                <applications_clocks>
                        <graphics_clock>N/A</graphics_clock>
                        <mem_clock>N/A</mem_clock>
                </applications_clocks>
                <default_applications_clocks>
                        <graphics_clock>N/A</graphics_clock>
                        <mem_clock>N/A</mem_clock>
                </default_applications_clocks>
                <max_clocks>
                        <graphics_clock>2115 MHz</graphics_clock>
                        <sm_clock>2115 MHz</sm_clock>
                        <mem_clock>7000 MHz</mem_clock>
                        <video_clock>1950 MHz</video_clock>
                </max_clocks>
                <max_customer_boost_clocks>
                        <graphics_clock>N/A</graphics_clock>
                </max_customer_boost_clocks>
                <clock_policy>
                        <auto_boost>N/A</auto_boost>
                        <auto_boost_default>N/A</auto_boost_default>
                </clock_policy>
                <supported_clocks>N/A</supported_clocks>
                <processes>
                </processes>
                <accounted_processes>
                </accounted_processes>
        </gpu>

</nvidia_smi_log>
`)

	output := GpuXml{}
	if err := xml.Unmarshal(input, &output); err != nil {
		t.Fatal(err)
	}
	if len(output.Gpu) != 2 {
		t.Fatal(fmt.Sprint(output))
	}
	if output.Gpu[0].Pci.PciBus != "3B" {
		t.Fatal(fmt.Sprint(output))
	}
	if output.Gpu[1].Pci.PciBus != "D8" {
		t.Fatal(fmt.Sprint(output))
	}
}

func TestGroupGpu(t *testing.T) {
	info, err := GroupGpu(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(info)
}
