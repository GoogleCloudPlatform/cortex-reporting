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
*/
CREATE TABLE IF NOT EXISTS `{{ project_id_src }}.{{ dataset_cdc_processed_ecc }}.calendar_date_dim` (
  Date DATE,
  DateInt INTEGER,
  DateStr STRING,
  DateStr2 STRING,
  CalYear INTEGER,
  CalSemester INTEGER,
  CalQuarter INTEGER,
  CalMonth INTEGER,
  CalWeek INTEGER,
  CalYearStr STRING,
  CalSemesterStr STRING,
  CalSemesterStr2 STRING,
  CalQuarterStr STRING,
  CalQuarterStr2 STRING,
  CalMonthLongStr STRING,
  CalMonthShortStr STRING,
  CalWeekStr STRING,
  DayNameLong STRING,
  DayNameShort STRING,
  DayOfWeek INTEGER,
  DayOfMonth INTEGER,
  DayOfQuarter INTEGER,
  DayOfSemester INTEGER,
  DayOfYear INTEGER,
  YearSemester STRING,
  YearQuarter STRING,
  YearMonth STRING,
  YearMonth2 STRING,
  YearWeek STRING,
  IsFirstDayOfYear BOOLEAN,
  IsLastDayOfYear BOOLEAN,
  IsFirstDayOfSemester BOOLEAN,
  IsLastDayOfSemester BOOLEAN,
  IsFirstDayOfQuarter BOOLEAN,
  IsLastDayOfQuarter BOOLEAN,
  IsFirstDayOfMonth BOOLEAN,
  IsLastDayOfMonth BOOLEAN,
  IsFirstDayOfWeek BOOLEAN,
  IsLastDayOfWeek BOOLEAN,
  IsLeapYear BOOLEAN,
  IsWeekDay BOOLEAN,
  IsWeekEnd BOOLEAN,
  WeekStartDate INTEGER,
  WeekEndDate INTEGER,
  MonthStartDate INTEGER,
  MonthEndDate INTEGER,
  Has53Weeks BOOLEAN
)
