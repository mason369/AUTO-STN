# AUTO-STN

## 项目介绍

AUTO-STN是一个用于网络设备自动化巡检的工具，专为STN-A设备设计。该工具可以自动连接到多台设备，执行预定义的命令集，收集设备状态信息，并生成报告。

当前版本：V2.6

### 最新更新
- 新增PTP时钟检查功能
- 修复若干BUG

## 安装指南

### 1. 确保有Python环境
前往 [Python官网](https://www.python.org/downloads/)，下载并安装Python 3.8或更高版本。
安装完成后，在终端中执行以下命令确认是否安装成功：

```shell
python --version
```

若安装成功，终端中会输出已安装的Python的版本号。

### 2. 安装依赖包
```bash
pip install openpyxl pytz paramiko tqdm colorama
```

## 使用方法

### 1. 运行程序

```bash
python engineerl-V2.6.py
```

### 2. 配置文件说明

#### stna-cmd1.csv
该文件包含要在设备上执行的命令列表。若运行1~5的命令或专项巡检子功能中的前4个，需使用到该文件。

示例内容：
```
screen-length 512
show device
show temperature
show fan
show laser
show interface main
show ospf neighbor brief
show ldp session
show mpls l2vc brief
show bfd session brief
show alarm current
screen-length 25
```

#### host-stna.csv
该文件包含要连接的设备信息，格式为：
```
设备IP,用户名,密码,命令文件,结果文本文件,结果CSV文件
```

示例：
```
192.168.1.1,admin,admin,stna-cmd1.csv,stna-result1.txt,stna-result1.csv
```

#### userhost-stna.csv
若运行执行15.自定义指令功能，需手动创建该文件，指定要执行的设备IP、用户名、密码，格式与host-stna.csv相同。

#### 自定义指令.txt
若运行执行15.自定义指令功能，需手动创建该文件，指定要执行的命令。

示例内容：
```
con
show fan
show temperature
fan speed percent 90
save
```

## 主要功能

1. 设备基本信息采集
2. 设备接口状态检查
3. 设备协议状态检查（OSPF、LDP、BFD等）
4. 设备硬件状态检查（温度、风扇、激光器等）
5. PTP时钟检查
6. 自定义命令执行

## 贡献

作者：杨茂森

## 许可

本项目遵循MIT许可证。
