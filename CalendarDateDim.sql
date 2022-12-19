# Copyright 2022 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

/*
* If_gen_ext is not true, the calendar_date_dim table will not be generated. This will generate the
* empty structure for views not to fail.
* This file will be moved to a common DAG generator in the next major release.
*/
{% if sql_flavour == 'ecc' -%}
{% include './ecc/CalendarDateDim.sql' -%}
{% endif -%}

{% if sql_flavour == 's4' -%}
{% include './s4/CalendarDateDim.sql' -%}
{% endif -%}



