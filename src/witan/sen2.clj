(ns witan.sen2
  "Special educational needs survey (SEN2) information

  from [gov.uk](https://www.gov.uk/guidance/special-educational-needs-survey) unless stated otherwise.")

(def census-dates
  "SEN2 census dates"
  (sorted-map
   2015 "2015-01-15" ; from [SEN2 2015 Guide v1.3](https://dera.ioe.ac.uk/21852/1/SEN2_2015_Guide_Version_1.3.pdf) (retrieved 2023-04-12)
   2016 "2016-01-21" ; from [SEN2 2016 Guide v1.3](https://dera.ioe.ac.uk/24874/1/SEN2_2016_Guide_Version_1.3.pdf) (retrieved 2023-04-12)
   2017 "2017-01-19" ; from [SEN2 2017 Guide v1.2](https://dera.ioe.ac.uk/27646/1/SEN2_2017_Guide_Version_1.2.pdf) (retrieved 2023-04-12)
   2018 "2018-01-18" ; from [SEN2 2018 Guide v1.2](https://dera.ioe.ac.uk/30003/1/SEN2_2018_Guide_Version_1.2.pdf) (retrieved 2023-04-12)
   2019 "2019-01-17" ; from [SEN2 2019 Guide v1.2](https://dera.ioe.ac.uk/32271/1/SEN2_2019_Guide_Version_1.2.pdf) (retrieved 2023-04-12)
   2020 "2020-01-16" ; from [SEN2 2020 Guide v1.1](https://dera.ioe.ac.uk/34219/1/SEN2_2020_Guide_Version_1.1.pdf) (retrieved 2023-04-12)
   2021 "2021-01-14" ; from [SEN2 2021 Guide v1.1](https://dera.ioe.ac.uk/36468/1/SEN2_2021_Guide_Version_1.1.pdf) (retrieved 2023-04-12)
   2022 "2022-01-20" ; from [SEN2 2022 Guide v1.1](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1013751/SEN2_2022_Guide.pdf) (retrieved 2023-04-12)
   2023 "2023-01-19" ; from [SEN2 2023 Guide v1.0](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1099346/2023_SEN2_Person_level_-_Guide_Version_1.0.pdf) (retrieved 2023-04-12)
   2024 "2024-01-18" ; from [SEN2 2024 Guide v1.0](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1174521/2024_SEN2_Person_level_-_Guide_Version_1.0.pdf) (retrieved 2023-07-28)
   ))
