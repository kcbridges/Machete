using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

using System.Data;
using System.Data.Entity;
using System.Data.Entity.Core.Objects;
using System.Data.SqlClient;

using Machete.Domain;
using Machete.Domain.Entities;

using Machete.Data;
using Machete.Data.Infrastructure;

using NLog;

namespace Machete.Service
{
    #region public class ReportService (Interface and Constructor)
    public interface IReportService
    {
        DailySumData DailySumController(DateTime date);
        IEnumerable<WeeklySumData> WeeklySumController(DateTime beginDate, DateTime endDate);
        IEnumerable<DispatchData> MonthlySumController(DateTime beginDate, DateTime endDate);
        IEnumerable<ActivityData> ActivityReportController(DateTime beginDate, DateTime endDate, string reportType);
        IEnumerable<EmployerModel> EmployerReportController(DateTime beginDate, DateTime endDate);
        IEnumerable<WorkerData> WorkerReportController(DateTime beginDate, DateTime endDate, string reportType);
    }

    public class ReportService : IReportService
    {
        protected readonly IWorkOrderRepository woRepo;
        protected readonly IWorkAssignmentRepository waRepo;
        protected readonly IWorkerRepository wRepo;
        protected readonly IWorkerSigninRepository wsiRepo;
        protected readonly IWorkerRequestRepository wrRepo;
        protected readonly ILookupRepository lookRepo;
        protected readonly ILookupCache lCache;
        protected readonly IEmployerRepository eRepo;
        protected readonly IActivitySigninRepository asRepo;

        public ReportService(IWorkOrderRepository woRepo,
                             IWorkAssignmentRepository waRepo,
                             IWorkerRepository wRepo,
                             IWorkerSigninRepository wsiRepo,
                             IWorkerRequestRepository wrRepo,
                             ILookupRepository lookRepo,
                             ILookupCache lCache,
                             IEmployerRepository eRepo,
                             IActivitySigninRepository asRepo)
        {
            this.woRepo = woRepo;
            this.waRepo = waRepo;
            this.wRepo = wRepo;
            this.wsiRepo = wsiRepo;
            this.wrRepo = wrRepo;
            this.lookRepo = lookRepo;
            this.lCache = lCache;
            this.eRepo = eRepo;
            this.asRepo = asRepo;
        }

    #endregion

        #region BasicFunctions
        /// <summary>
        /// A simple count of worker signins for the given period.
        /// </summary>
        /// <param name="beginDate"></param>
        /// <param name="endDate"></param>
        /// <returns>IQueryable of type ReportUnit </returns>
        public IQueryable<ReportUnit> CountSignins()
        {
            var wsiQ = wsiRepo.GetAllQ();

            return wsiQ
            .GroupBy(g => DbFunctions.TruncateTime(g.dateforsignin))
            .Select(h => new ReportUnit
            {
                date = h.Key,
                count = h.Count()
            })
            .OrderBy(o => o.date);
        }

        public IQueryable<ReportUnit> CountUniqueSignins()
        {
            var wsiQ = wsiRepo.GetAllQ();

            return wsiQ
                .GroupBy(g => g.dwccardnum)
                .Select(h => new
                {
                    info = h.Key.ToString(),
                    date = h.Min(x => x.dateforsignin)
                })
                .GroupBy(g => g.date)
                .Select(y => new ReportUnit
                {
                    date = y.Key,
                    count = y.Count()
                })
                .AsQueryable();

        }
        /// <summary>
        /// A simple count of unduplicated worker signins for the given period.
        /// Note: Casa's policy is that these should reset on beginDate, but that
        /// isn't truly "unduplicated" within the program.
        /// </summary>
        /// <param name="range">IEnumerable<DateTime>, not null</param>
        /// <returns>int</returns>
        public IQueryable<ReportUnit> CountUniqueSignins(IEnumerable<DateTime> range)
        {
            var wsiQ = wsiRepo.GetAllQ();

            return range
                    .GroupJoin(
                        wsiQ
                        .GroupBy(g => g.dwccardnum)
                        .Select(h => new { dwc = h.Key, fsi = h.Min(x => x.dateforsignin) }),
                        x => x,
                        y => DbFunctions.TruncateTime(y.fsi),
                        (x, y) => new ReportUnit
                        {
                            date = x,
                            count = y
                                .GroupBy(g => g.dwc)
                                .Select(z => new
                                {
                                    dwc = z.Key
                                }).Count()
                        })
                    .OrderBy(o => o.date)
                    .AsQueryable();
        }

        /// <summary>
        /// Counts work assignments by date (assigned orders only).
        /// </summary>
        /// <returns></returns>
        public IQueryable<ReportUnit> CountAssignments()
        {
            var waQ = waRepo.GetAllQ();

            return waQ
                .Where(y => y.workerAssignedID.HasValue)
                .GroupBy(gb => DbFunctions.TruncateTime(gb.workOrder.dateTimeofWork))
                .Select(g => new ReportUnit
                {
                    date = g.Key,
                    count = g.Count(),
                    info = ""
                });
        }

        /// <summary>
        /// Counts permanent or temporary work assignments by date.
        /// </summary>
        /// <param name="perm">bool</param>
        /// <returns>IQueryable<ReportUnit></returns>
        public IQueryable<ReportUnit> CountAssignments(bool perm)
        {
            var waQ = waRepo.GetAllQ();

            if(perm) return waQ
                .Where(y => y.workerAssignedID.HasValue
                    && y.workOrder.permanentPlacement)
                .GroupBy(gb => DbFunctions.TruncateTime(gb.workOrder.dateTimeofWork))
                .Select(g => new ReportUnit
                {
                    date = g.Key,
                    count = g.Count(),
                    info = ""
                });
            else return waQ
                .Where(y => y.workerAssignedID.HasValue
                    && !y.workOrder.permanentPlacement)
                .GroupBy(gb => DbFunctions.TruncateTime(gb.workOrder.dateTimeofWork))
                .Select(g => new ReportUnit
                {
                    date = g.Key,
                    count = g.Count(),
                    info = ""
                });
        }

        /// <summary>
        /// Counts unduplicated work assignments for a given time period.
        /// </summary>
        /// <param name="range">IEnumerable<DateTime></param>
        /// <returns></returns>
        public IQueryable<ReportUnit> CountAssignments(IEnumerable<DateTime> range)
        {
            var waQ = waRepo.GetAllQ();

            return range
                .GroupJoin(waQ
                    .Where(w => w.workOrder.dateTimeofWork >= range.First())
                    .GroupBy(g => g.workerAssignedID)
                    .Select(h => new { waID = h.Key, fwa = h.Min(x => x.workOrder.dateTimeofWork) }),
                x => x,
                wa => wa.fwa.Date,
                (x, wa) => new ReportUnit
                {
                    date = x,
                    count = wa
                        .GroupBy(g => g.waID)
                        .Select(z => z.Key)
                        .Count()
                })
                .AsQueryable();
        }


        /// <summary>
        /// Counts ***unassigned*** work assignments by date where the order status matches the given key.
        /// </summary>
        /// <param name="orderStatus">The string to use as key</param>
        /// <returns></returns>
        public IQueryable<ReportUnit> CountNotAssigned(string orderStatus)
        {
            var waQ = waRepo.GetAllQ();

            return waQ
                .Where(y => y.workerAssignedID == null
                    && y.workOrder.status == lCache.getByKeys(LCategory.orderstatus, orderStatus))
                .GroupBy(gb => DbFunctions.TruncateTime(gb.workOrder.dateTimeofWork))
                .Select(g => new ReportUnit
                {
                    date = g.Key,
                    count = g.Count(),
                    info = ""
                });
        }

        /// <summary>
        /// CountCancelled()
        /// </summary>
        /// <returns>Returns a count of cancelled orders by date.</returns>
        public IQueryable<ReportUnit> CountCancelled()
        {
            var woQ = woRepo.GetAllQ();

            return woQ.Where(whr => whr.status == WorkOrder.iCancelled)
                .GroupBy(gb => DbFunctions.TruncateTime(gb.dateTimeofWork))
                .Select(g => new ReportUnit
                {
                    date = g.Key,
                    count = g.Count(),
                });
        }

        /// <summary>
        /// Counts by type of dispatch (DWC, HHH, Propio/ea.). Very Casa Latina specific, but these
        /// numbers can also be used by other centers, especially where they have women's programs.
        /// </summary>
        /// <param name="dateRequested">A single DateTime parameter</param>
        /// <returns>IQueryable</returns>
        public IQueryable<TypeOfDispatchModel> CountTypeofDispatch()
        {
            var waQ = waRepo.GetAllQ();
            var dwcId = lCache.getByKeys("worktype", "DWC"); 
            var hhhId = lCache.getByKeys("worktype", "HHH");

            return waQ.Select(x => new
            {
                date = x.workOrder.dateTimeofWork.Date,
                dwc = x.workerAssigned.typeOfWorkID == null ? 0
                        : x.workerAssigned.typeOfWorkID == dwcId ? 1
                        : 0,
                dwcr = x.workerAssigned.typeOfWorkID == null ? 0
                        : x.workerAssigned.typeOfWorkID == dwcId
                        ? x.workOrder.workerRequests == null ? 0
                        : x.workOrder.workerRequests.Select(y => y.WorkerID).Contains(x.workerAssigned.ID) ? 1
                        : 0 : 0,
                hhh = x.workerAssigned.typeOfWorkID == null ? 0
                        : x.workerAssigned.typeOfWorkID == hhhId ? 1
                        : 0,
                hhhr = x.workerAssigned.typeOfWorkID == null ? 0
                        : x.workerAssigned.typeOfWorkID == hhhId
                        ? x.workOrder.workerRequests == null ? 0
                        : x.workOrder.workerRequests.Select(y => y.WorkerID).Contains(x.workerAssigned.ID) ? 1
                        : 0 : 0,
            })
            .GroupBy(g => g.date)
            .Select(x => new TypeOfDispatchModel
            {
                date = x.Key,
                dwcount = x.Sum(y => y.dwc),
                dwcountr = x.Sum(y => y.dwcr),
                hhcount = x.Sum(y => y.hhh),
                hhcountr = x.Sum(y => y.hhhr)
            })
            .OrderBy(o => o.date);
        }

        /// <summary>
        /// Grabs a sum of hours and wages and averages them for a given time period.
        /// </summary>
        /// <param name="beginDate">Start date for the query.</param>
        /// <param name="endDate">End date for the query.</param>
        /// <returns></returns>
        public IQueryable<AverageWageModel> HourlyWageAverage()
        {
            var waQ = waRepo.GetAllQ();
            
            return waQ
                .GroupBy(gb => (DateTime)DbFunctions.TruncateTime(gb.workOrder.dateTimeofWork))
                .Select(x => new AverageWageModel
                {
                    date = x.Key,
                    hours = x.Sum(y => y.hours),
                    wages = Math.Round(x.Sum(y => y.hourlyWage * y.hours), 2, MidpointRounding.AwayFromZero),
                    avg = Math.Round(x.Sum(y => y.hourlyWage * y.hours) / x.Sum(y => y.hours), 2, MidpointRounding.AwayFromZero)
                })
                .OrderBy(o => o.date);
        }

        /// <summary>
        /// Lists jobs in order of occurrence by date.
        /// </summary>
        /// <returns>IQueryable<ReportUnit></returns>
        public IQueryable<ReportUnit> ListJobs()
        {
            var waQ = waRepo.GetAllQ();

            return waQ
                .GroupBy(gb => new {
                    date = DbFunctions.TruncateTime(gb.workOrder.dateTimeofWork),
                    type = gb.skillID
                })
                .Select(group => new ReportUnit
                {
                    date = group.Key.date,
                    count = group.Count(),
                    info = lCache.getByID(group.Key.type).text_EN
                })
                .OrderBy(o => o.date)
                .ThenByDescending(t => t.count);
        }

        /// <summary>
        /// Lists jobs in order of occurrence by date and zip code.
        /// Data can then be aggregated any number of ways; count of
        /// all orders by zip, top jobs for each zip, top jobs for
        /// each zip for each date, etc.
        /// 
        /// Count is Work Assignments. Counts only dispatched WAs.
        /// ZipUnit is a modified version of ReportUnit that includes
        /// a field for zips.
        /// </summary>
        /// <returns>IQueryable<ZipUnit></returns>
        public IQueryable<ZipUnit> ListJobsByZip()
        {
            var waQ = waRepo.GetAllQ();

            return waQ
                .Where(y => y.workerAssignedID.HasValue)
                .GroupBy(gb => new
                {
                    date = DbFunctions.TruncateTime(gb.workOrder.dateTimeofWork),
                    zip = gb.workOrder.zipcode,
                    type = gb.skillID
                })
                .Select(group => new ZipUnit
                {
                    date = group.Key.date,
                    zip = group.Key.zip,
                    count = group.Count(),
                    info = lCache.getByID(group.Key.type).text_EN
                })
                .OrderBy(o => o.date)
                .ThenByDescending(t => t.count);
        }

        /// <summary>
        /// Lists jobs in order of occurrence by date and zip code.
        /// Data can then be aggregated any number of ways; count of
        /// all orders by zip, top jobs for each zip, top jobs for
        /// each zip for each date, etc.
        /// 
        /// Count is Work Orders. Counts only completed Work Orders.
        /// </summary>
        /// <returns>IQueryable<ZipUnit></returns>
        public IQueryable<ReportUnit> ListOrdersByZip()
        {
            var woQ = woRepo.GetAllQ();

            return woQ
                .Where(y => y.status == lCache.getByKeys(LCategory.orderstatus, LOrderStatus.Completed))
                .GroupBy(gb => new
                {
                    date = DbFunctions.TruncateTime(gb.dateTimeofWork),
                    zip = gb.zipcode
                })
                .Select(group => new ReportUnit
                {
                    date = group.Key.date,
                    count = group.Count(),
                    info = group.Key.zip
                })
                .OrderBy(o => o.date)
                .ThenByDescending(t => t.count);
        }

        /// <summary>
        /// Returns all activity signins grouped by day,
        /// then activityName. Members can then aggregate
        /// that information by grouping on "info" (activityName)
        /// and/or specifying a limiting date range.
        /// </summary>
        /// <returns>IQueryable<ReportUnit></returns>
        public IQueryable<ReportUnit> GetActivitySignins()
        {
            var asQ = asRepo.GetAllQ();

            return asQ
                .Where(signin => signin.person != null)
                .GroupBy(gb => new { 
                    date = DbFunctions.TruncateTime(gb.Activity.dateStart), 
                    name = lCache.getByID(gb.Activity.name).text_EN })
                .Select(grouping => new ReportUnit
                {
                    date = grouping.Key.date,
                    info = grouping.Key.name,
                    count = grouping.Count()
                })
                .OrderBy(o => o.date)
                .ThenByDescending(t => t.count);
        }

        /// <summary>
        /// Returns unique activity signins for a given time period;
        /// this method answers the question, "How many individuals
        /// attended (any) class for a given time period?"
        /// 
        /// See third override for limiting on specific activityNames.
        /// </summary>
        /// <returns>IQueryable<ReportUnit></returns>
        public IQueryable<ReportUnit> GetActivitySignins(IEnumerable<DateTime> range)
        {
            var asQ = asRepo.GetAllQ();

            return range.GroupJoin(asQ
                .Where(w => w.Activity.dateStart >= range.First())
                .GroupBy(g => g.personID)
                .Select(h => new { waID = h.Key, fwa = h.Min(x => x.Activity.dateStart) }),
                x => x,
                wa => DbFunctions.TruncateTime(wa.fwa),//.Date,
                (x, wa) => new ReportUnit
                {
                    date = x,
                    count = wa
                        .GroupBy(g => g.waID)
                        .Select(z => z.Key)
                        .Count()
                })
                .AsQueryable();
        }

        /// <summary>
        /// Returns unique activity signins for a given time period;
        /// this method answers the question, "How many individuals
        /// attended a specific type of class for a given time period?"
        /// </summary>
        /// <param name="actNameId">The Lookups ID of the ActivityName to filter by.</param>
        /// <returns>IQueryable<ReportUnit></returns>
        public IQueryable<ReportUnit> GetActivitySignins(IEnumerable<DateTime> range, int actNameId)
        {
            var asQ = asRepo.GetAllQ();

            return range.GroupJoin(asQ
                    .Where(w => w.Activity.dateStart >= new DateTime(2014, 3, 1)
                        && w.Activity.name == actNameId)
                    .GroupBy(g => g.personID)
                    .Select(h => new { waID = h.Key, fwa = h.Min(x => x.Activity.dateStart) }),
                x => x,
                wa => DbFunctions.TruncateTime(wa.fwa),
                (x, wa) => new ReportUnit
                {
                    date = x,
                    count = wa
                        .GroupBy(g => g.waID)
                        .Select(z => z.Key)
                        .Count()
                })
                .AsQueryable();
        }

        /// <summary>
        /// Returns dates of completion, minutes completed, and the dwccardnum for
        /// all adults attending >x minutes of a particular class for a given time range.
        /// </summary>
        /// <param name="actNameId">The Lookups ID of the activityName to filter by.</param>
        /// <param name="minutesInClass">The amount of time, in minutes, those to be counted must have attended classes of the given activityName type.</param>
        /// <returns>date, dwccardnum, minutesInClass</returns>
        public IQueryable<ReportUnit> GetActivityRockstars(IEnumerable<DateTime> range, int actNameId, int minutesInClass)
        {
            var asQ = asRepo.GetAllQ();

            return range
            .GroupJoin(asQ
                .Where(w => w.Activity.name == actNameId)
                .GroupBy(gb => gb.dwccardnum)
                .Select(asi => new
                {
                    dwc = asi.Key,
                    date = DbFunctions.TruncateTime(asi.Select(sel => sel.dateforsignin).Max()),
                    // Get total minutes of class person has
                    // attended for this activityName:
                    mins = asi
                        .Sum(x => 
                            ((DbFunctions.DiffHours
                                (x.Activity.dateEnd,
                                x.Activity.dateStart)
                                * 60)
                            + DbFunctions.DiffMinutes
                                (x.Activity.dateEnd, 
                                x.Activity.dateStart)))
                })
                .Where(w => w.mins >= minutesInClass),
                x => x,
                minQ => minQ.date,
                (x, minQ) => new ReportUnit {
                    date = x,
                    count = minQ.Count(),
                    info = lCache.getByID(actNameId).text_EN
                })
                .AsQueryable();
        }

        /// <summary>
        /// Returns dates of completion, minutes completed, and the dwccardnum for
        /// all adults attending >x minutes of an array of activityName IDs for a given time range.
        /// </summary>
        /// <param name="actNameId">The Lookups ID of the activityName to filter by.</param>
        /// <param name="minutesInClass">The amount of time, in minutes, those to be counted must have attended classes of the given activityName type.</param>
        /// <returns>date, dwccardnum, minutesInClass</returns>
        public IQueryable<ReportUnit> GetActivityRockstars(IEnumerable<DateTime> range, int[] actNameId, int minutesInClass)
        {
            var asQ = asRepo.GetAllQ();
            string[] naQ = lookRepo.GetManyQ(z => actNameId.Contains(z.ID)).Select(na => na.text_EN + ", ").ToArray();
            string sb = new StringBuilder().Append(naQ).ToString(); // should show all actNames being queried
            sb.TrimEnd(sb[sb.Length - 2]);

            return range
            .GroupJoin(asQ
                .Where(w => actNameId.Contains(w.Activity.name))
                .GroupBy(gb => gb.dwccardnum)
                .Select(asi => new
                {
                    dwc = asi.Key,
                    date = DbFunctions.TruncateTime(asi.Select(sel => sel.dateforsignin).Max()),
                    // Get total minutes of class person has
                    // attended for this activityName:
                    mins = asi
                        .Sum(x =>
                            ((DbFunctions.DiffHours
                                (x.Activity.dateEnd,
                                x.Activity.dateStart)
                                * 60)
                            + DbFunctions.DiffMinutes
                                (x.Activity.dateEnd,
                                x.Activity.dateStart)))
                })
                .Where(w => w.mins >= minutesInClass),
                x => x,
                minQ => minQ.date,
                (x, minQ) => new ReportUnit
                {
                    date = x,
                    count = minQ.Count(),
                    info = sb
                })
                .AsQueryable();
        }

        /// <summary>
        /// Returns a count of new, expired, and still active members by enumerated dates within the given period.
        /// This is a resource intensive query, because it targets specific data with daily granularity; this usage 
        /// can be offset by reducing the interval and increasing the unit of measure.
        /// </summary>
        /// <param name="beginDate"></param>
        /// <param name="endDate"></param>
        /// <param name="unitOfMeasure">The unit of time to measure; "days" or "months".</param>
        /// <param name="interval">The interval of time (7 days, 3 months) as int</param>
        /// <returns>date, enrolledOnDate, expiredOnDate, count</returns>
        public IQueryable<StatusUnit> MemberStatusByDate(IEnumerable<DateTime> range)
        {
            var wQ = wRepo.GetAllQ();

            return range.GroupJoin(wQ,
                x => x,
                y => y.dateOfMembership,
                (x, y) => new
                {
                    date = x,
                    count = wQ.Where(z => z.dateOfMembership < x && z.memberexpirationdate > x).Count(),
                    enrolledOnDate = y.Count()
                }
                )
            .GroupJoin(wQ,
                x => x.date,
                y => y.memberexpirationdate,
                (x, y) => new StatusUnit
                {
                    date = x.date,
                    count = x.count,
                    enrolledOnDate = x.enrolledOnDate,
                    expiredOnDate = y.Count()
                })
            .AsQueryable();
        }

        public IQueryable<MemberDateModel> SingleAdults()
        {
            IQueryable<MemberDateModel> query;

            var wQ = wRepo.GetAllQ();

            query = wQ
                .Where(worker => !worker.livewithchildren && worker.maritalstatus != lCache.getByKeys(LCategory.maritalstatus, LMaritalStatus.Married))
                .Select(x => new MemberDateModel
                {
                    dwcnum = x.dwccardnum,
                    zip = x.Person.zipcode,
                    memDate = x.dateOfMembership,
                    expDate = x.memberexpirationdate
                });

            return query;
        }

        public IQueryable<MemberDateModel> FamilyHouseholds()
        {
            IQueryable<MemberDateModel> query;

            var lQ = lookRepo.GetAllQ();
            var wQ = wRepo.GetAllQ();

            query = wQ
                .Where(worker => worker.livewithchildren && worker.maritalstatus == lCache.getByKeys(LCategory.maritalstatus, LMaritalStatus.Married))
                .Select(x => new MemberDateModel
                {
                    dwcnum = x.dwccardnum,
                    zip = x.Person.zipcode,
                    memDate = x.dateOfMembership,
                    expDate = x.memberexpirationdate
                });

            return query;
        }

        public IQueryable<ReportUnit> ClientProfileHomeless(IEnumerable<DateTime> range)
        {
            var wQ = wRepo.GetAllQ();

            return range
                .Select(x => new ReportUnit
                {
                    date = x,
                    count = wQ.Where(w => w.memberexpirationdate > x && w.dateOfMembership <= x && w.homeless == true).Count()
                })
                .AsQueryable();
        }

        public IQueryable<ReportUnit> ClientProfileHouseholdComposition(DateTime beginDate, DateTime endDate)
        {
            IQueryable<ReportUnit> query;


            var wQ = wRepo.GetAllQ();
            var lQ = lookRepo.GetAllQ();

            query = wQ
                .Where(whr => whr.memberexpirationdate > beginDate && whr.dateOfMembership < endDate)
                .GroupBy(grp => new
                {
                    maritalStatus = grp.maritalstatus,
                    withChildren = grp.livewithchildren
                })
                .Join(lQ,
                    gJoin => gJoin.Key.maritalStatus,
                    lJoin => lJoin.ID,
                    (gJoin, lJoin) => new
                    {
                        maritalStatus = lJoin.text_EN,
                        withChildren = gJoin.Key.withChildren ? "With Children" : "Without Children",
                        count = gJoin.Count()
                    })
                .Select(glJoin => new ReportUnit
                {
                    info = glJoin.maritalStatus + ", " + glJoin.withChildren,
                    count = glJoin.count
                });

            return query;
        }

        public IQueryable<ReportUnit> ClientProfileIncome(DateTime beginDate, DateTime endDate)
        {
            IQueryable<ReportUnit> query;


            var wQ = wRepo.GetAllQ();
            var lQ = lookRepo.GetAllQ();

            query = wQ
                .Where(whr => whr.memberexpirationdate > beginDate && whr.dateOfMembership < endDate)
                .Join(lQ,
                    wJoin => wJoin.incomeID,
                    lJoin => lJoin.ID,
                    (gJoin, lJoin) => new
                    {
                        incomeLevel = lJoin.text_EN == "Less than $15,000" ? "Very low (< 30% median)" :
                                        lJoin.text_EN == "Between $15,000 and $25,000" ? "Moderate (> 50% median)" :
                                        lJoin.text_EN != "unknown" ? "Above moderate (> 80% median)" :
                                        "Unknown"
                    })
                .GroupBy(grp => grp.incomeLevel)
                .Select(group => new ReportUnit
                {
                    info = group.Key,
                    count = group.Count()
                })
                .OrderBy(ob => ob.count);

            return query;
        }

        public IQueryable<ReportUnit> ClientProfileWorkerAge(DateTime beginDate, DateTime endDate)
        {
            IQueryable<ReportUnit> query;


            var wQ = wRepo.GetAllQ();

            query = wQ
                .Where(whr => whr.memberexpirationdate > beginDate && whr.dateOfMembership < endDate)
                .Select(worker => new
                {
                    age = (new DateTime(1753, 1, 1) + (DateTime.Now - worker.dateOfBirth)).Year - 1753,
                    dob = worker.dateOfBirth
                })
                .Select(ageO => ageO.dob == new DateTime(1900, 1, 1) ? "Unknown" :
                                ageO.age <= 5 ? "0 to 5 years" :
                                ageO.age <= 12 ? "6 to 12 years" :
                                ageO.age <= 18 ? "13 to 18 years" :
                                ageO.age <= 29 ? "19 to 29 years" :
                                ageO.age <= 45 ? "30 to 45 years" :
                                ageO.age <= 64 ? "46 to 64 years" :
                                ageO.age <= 84 ? "65 to 84 years" :
                                "85+ years")
                .GroupBy(ageCategory => ageCategory)
                .Select(group => new ReportUnit
                {
                    info = group.Key,
                    count = group.Count()
                });

            return query;
			
        }

        public IQueryable<ReportUnit> ClientProfileGender(DateTime beginDate, DateTime endDate)
        {
            IQueryable<ReportUnit> query;


            var wQ = wRepo.GetAllQ();
            var lQ = lookRepo.GetAllQ();

            query = wQ
                .Where(whr => whr.memberexpirationdate > beginDate && whr.dateOfMembership < endDate)
                .GroupBy(worker => worker.Person.gender)
                .Join(lQ,
                    group => group.Key,
                    look => look.ID,
                    (group, look) => new ReportUnit
                    {
                        count = group.Count(),
                        info = look.text_EN
                    });

            return query;
        }

        public IQueryable<ReportUnit> ClientProfileHasDisability(DateTime beginDate, DateTime endDate)
        {
            IQueryable<ReportUnit> query;


            var wQ = wRepo.GetAllQ();

            query = wQ
                .Where(whr => whr.memberexpirationdate > beginDate && whr.dateOfMembership < endDate)
                .GroupBy(worker => worker.disabled)
                .Select(group => new ReportUnit
                {
                    info = group.Key ? "Yes" : "No",
                    count = group.Count()
                });

            return query;
        }

        public IQueryable<RaceUnit> ClientProfileRaceEthnicity(IEnumerable<DateTime> range)
        {
            IQueryable<RaceUnit> query;


            var wQ = wRepo.GetAllQ();
            var lQ = lookRepo.GetAllQ();


            return range
                .Select(x => new RaceUnit
                   {
                       date = x,
                       afroamerican = wQ.Where(wo => wo.RaceID == lQ.Single(w => w.category == "race" && w.text_EN == "Afroamerican").ID && wo.memberexpirationdate > x && wo.dateOfMembership <= x).Count(),
                       asian = wQ.Where(wo => wo.RaceID == lQ.Single(w => w.category == "race" && w.text_EN == "Asian").ID && wo.memberexpirationdate > x && wo.dateOfMembership <= x).Count(),
                       caucasian = wQ.Where(wo => wo.RaceID == lQ.Single(w => w.category == "race" && w.text_EN == "Caucasian").ID && wo.memberexpirationdate > x && wo.dateOfMembership <= x).Count(),
                       hawaiian = wQ.Where(wo => wo.RaceID == lQ.Single(w => w.category == "race" && w.text_EN == "Hawaiian").ID && wo.memberexpirationdate > x && wo.dateOfMembership <= x).Count(),
                       latino = wQ.Where(wo => wo.RaceID == lQ.Single(w => w.category == "race" && w.text_EN == "Latino").ID && wo.memberexpirationdate > x && wo.dateOfMembership <= x).Count(),
                       nativeamerican = wQ.Where(wo => wo.RaceID == lQ.Single(w => w.category == "race" && w.text_EN == "Native American").ID && wo.memberexpirationdate > x && wo.dateOfMembership <= x).Count(),
                       other = wQ.Where(wo => wo.RaceID == lQ.Single(w => w.category == "race" && w.text_EN == "Other").ID && wo.memberexpirationdate > x && wo.dateOfMembership <= x).Count()
                   }).AsQueryable();

            return query;
        }

        public IQueryable<ReportUnit> ClientProfileRefugeeImmigrant(IEnumerable<DateTime> range)
        {
            IQueryable<ReportUnit> query;


            var wQ = wRepo.GetAllQ();
            var lQ = lookRepo.GetAllQ();

            return range
                .Select(x => new ReportUnit
           {
               date = x,
               count = wQ.Where(w => w.memberexpirationdate.Date > x && w.dateOfMembership.Date <= x && w.immigrantrefugee == true).Count(),
           }).AsQueryable();

            return query;
        }

        public IQueryable<ReportUnit> ClientProfileEnglishLevel(DateTime beginDate, DateTime endDate)
        {
            IQueryable<ReportUnit> query;


            var wQ = wRepo.GetAllQ();

            query = wQ
                .Where(whr => whr.memberexpirationdate > beginDate && whr.dateOfMembership < endDate)
                .GroupBy(worker => worker.englishlevelID)
                .Select(group => new ReportUnit
                {
                    info = "English " + group.Key,
                    count = group.Count()
                })
                .OrderBy(ob => ob.info);

            return query;
        }

        #endregion

        #region ReportData

        /// <summary>
        /// Controller for daily summary report.
        /// </summary>
        /// <param name="beginDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public DailySumData DailySumController(DateTime date)
        {
            TypeOfDispatchModel dclCurrent;
            IEnumerable<ReportUnit> dailySignins;
            IEnumerable<ReportUnit> dailyUnique;
            IEnumerable<ReportUnit> dailyAssignments;
            IEnumerable<ReportUnit> dailyCancelled;
            DailySumData q;

            dclCurrent = CountTypeofDispatch().ToList().Where(x => x.date == date).FirstOrDefault();
            dailySignins = CountSignins().ToList();
            dailyUnique = CountUniqueSignins().ToList();
            dailyAssignments = CountAssignments().ToList();
            dailyCancelled = CountCancelled().ToList();

            q = new DailySumData
                {
                    date = date,
                    dwcount = dclCurrent.dwcount,
                    dwcountr = dclCurrent.dwcountr,
                    hhcount = dclCurrent.hhcount,
                    hhcountr = dclCurrent.hhcountr,
                    uniqueSignins = (int)dailyUnique.Where(whr => whr.date == date).Select(g => g.count).FirstOrDefault(),
                    totalSignins = (int)dailySignins.Where(whr => whr.date == date).Select(g => g.count).FirstOrDefault(),
                    totalAssignments = (int)dailyAssignments.Where(whr => whr.date == date).Select(g => g.count).FirstOrDefault(),
                    cancelledJobs = (int)dailyCancelled.Where(whr => whr.date == date).Select(g => g.count).FirstOrDefault()
                };

            return q;
        }

        /// <summary>
        /// Controller for weekly summary report.
        /// </summary>
        /// <param name="beginDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IEnumerable<WeeklySumData> WeeklySumController(DateTime beginDate, DateTime endDate)
        {
            IEnumerable<AverageWageModel> weeklyWages;
            IEnumerable<ReportUnit> weeklySignins;
            IEnumerable<ReportUnit> weeklyAssignments;
            IEnumerable<ReportUnit> weeklyJobs;
            IEnumerable<WeeklySumData> q;

            var dateRange = GetDateRange(beginDate, endDate);

            weeklyWages = HourlyWageAverage().ToList();
            weeklySignins = CountSignins().ToList();
            weeklyAssignments = CountAssignments().ToList();
            weeklyJobs = ListJobs().ToList();

            q = weeklyWages
                .Select(g => new WeeklySumData
                {
                    dayofweek = g.date.DayOfWeek,
                    date = g.date,
                    totalSignins = (int)weeklySignins.Where(whr => whr.date == g.date).Select(h => h.count).FirstOrDefault(),
                    numWeekJobs = (int)weeklyAssignments.Where(whr => whr.date == g.date).Select(h => h.count).FirstOrDefault(),
                    weekEstDailyHours = g.hours,
                    weekEstPayment = g.wages,
                    weekHourlyWage = g.avg,
                    topJobs = weeklyJobs.Where(whr => whr.date == g.date)
                });

            return q;
        }

        public IEnumerable<DispatchData> DispatchReportController(DateTime beginDate, DateTime endDate)
        {
            IEnumerable<ReportUnit> signins;
            IEnumerable<ReportUnit> unique;
            IEnumerable<ReportUnit> classes;
            IEnumerable<ReportUnit> notAssigned;
            IEnumerable<ReportUnit> dispatched;
            IEnumerable<ReportUnit> tempDisp;
            IEnumerable<ReportUnit> permDisp;
            IEnumerable<ReportUnit> undupDisp;
            IEnumerable<AverageWageModel> average;
            IEnumerable<ReportUnit> cancelled;
            IEnumerable<ReportUnit> skills;

            IEnumerable<DispatchData> q;

            var dateRange = GetDateRange(beginDate, endDate);

            signins = CountSignins().ToList();
            unique = CountUniqueSignins().ToList();
            classes = GetActivitySignins().ToList();
            notAssigned = CountNotAssigned(LOrderStatus.Completed).ToList();
            dispatched = CountAssignments().ToList();
            tempDisp = CountAssignments(false).ToList();
            permDisp = CountAssignments(true).ToList();
            undupDisp = CountAssignments(dateRange).ToList();
            average = HourlyWageAverage().ToList();
            cancelled = CountNotAssigned(LOrderStatus.Cancelled).ToList();
            skills = ListJobs().ToList();

            q = dateRange
                .Select(g => new DispatchData
                {
                    dateStart = g,
                    dateEnd = g.AddDays(1),
                    totalSignins = (int)signins.Where(w => w.date == g).Select(h => h.count).FirstOrDefault(),
                    uniqueSignins = (int)unique.Where(w => w.date == g).Select(h => h.count).FirstOrDefault(), //dd
                    dispatched = (int)dispatched.Where(w => w.date == g).Select(h => h.count).FirstOrDefault(),
                    tempDispatched = (int)tempDisp.Where(w => w.date == g).Select(h => h.count).FirstOrDefault(), //dd
                    permanentPlacements = (int)permDisp.Where(w => w.date == g).Select(h => h.count).FirstOrDefault(), //dd
                    undupDispatched = (int)undupDisp.Where(w => w.date == g).Select(h => h.count).FirstOrDefault(), //dd
                    totalHours = average.Where(w => w.date == g).Select(h => h.hours).FirstOrDefault(),
                    totalIncome = average.Where(w => w.date == g).Select(h => h.wages).FirstOrDefault(),
                    avgIncomePerHour = average.Where(w => w.date == g).Select(h => h.avg).FirstOrDefault(),
                    countNotAssigned = (int)notAssigned.Where(w => w.date == g).Select(h => h.count).FirstOrDefault(),
                    cancelledAssignments = (int)cancelled.Where(w => w.date == g).Select(h => h.count).FirstOrDefault(),
                    skills = skills.Where(w => w.date == g)
                    
                });

            return q;
        }

        public IEnumerable<ActivityData> ActivityReportController(DateTime beginDate, DateTime endDate)
        {
            IEnumerable<ReportUnit> name;
            IEnumerable<ReportUnit> attendance;
            IEnumerable<ReportUnit> morethanxhours;
            IEnumerable<ActivityData> q;

            var dateRange = GetDateRange(beginDate, endDate);

            name = GetActivitySignins().ToList();
            attendance = GetActivitySignins().ToList();
            morethanxhours = GetActivitySignins().ToList();

            q = dateRange
                .Select(g => new ActivityData
                {
                    ActivityName = name.Where(w => w.date == g).ToString(),
                    Attendance = (int)attendance.Where(w => w.date == g).Select(a => a.count).FirstOrDefault(),
                    MoreThanXHours = (int)morethanxhours.Where(w => w.date == g).Select(a => a.count).FirstOrDefault(),
                    
                });

            return q;
        }

        /// <summary>
        /// NewWorkerController returns an IEnumerable containing the counts of single members, new single members, family
        /// members, and new family members. It does not include zip code completeness, which must be called separately.
        /// </summary>
        /// <param name="beginDate"></param>
        /// <param name="endDate"></param>
        /// <returns>date, singleAdults, familyHouseholds, newSingleAdults, newFamilyHouseholds, zipCodeCompleteness</returns>
        public IEnumerable<WorkerData> WorkerReportController(DateTime beginDate, DateTime endDate, string reportType)
        {
            IEnumerable<WorkerData> q;
            //IEnumerable<MemberDateModel> singleAdults;
            //IEnumerable<MemberDateModel> familyHouseholds;
            IEnumerable<ReportUnit> status;
            //IEnumerable<MemberDateModel> livesAlone;
            //IEnumerable<MemberDateModel> maritalStatus;
            IEnumerable<ReportUnit> enrolled;
            IEnumerable<ReportUnit> exited;
            //IEnumerable<ReportUnit> immigrantRefugee;
            //IEnumerable<ReportUnit> homeless;
            //IEnumerable<ReportUnit> skillsbreakdown;
            //IEnumerable<ReportUnit> englishLevel;
            //IEnumerable<ReportUnit> disabled;
            //IEnumerable<ReportUnit> license;
            //IEnumerable<ReportUnit> insurance;
            //IEnumerable<ReportUnit> age;
            //IEnumerable<ReportUnit> incomeLevel;
            //IEnumerable<ReportUnit> gender;

            var dateRange = GetDateRange(beginDate, endDate);

            //TODO: fix the methods that are red here, and also add new methods where they don't exist
            //singleAdults = SingleAdults().ToList();
            //familyHouseholds = FamilyHouseholds().ToList();
            status = MemberStatusByDate(dateRange).ToList();
            //livesAlone = FamilyHouseholds().ToList();
            enrolled = MemberStatusByDate(dateRange).ToList();
            exited = MemberStatusByDate(dateRange).ToList();
            //immigrantRefugee = ClientProfileRefugeeImmigrant(dateRange).ToList();
            //homeless = ClientProfileHomeless(dateRange).ToList();
            //skillsbreakdown =
            //englishLevel = ClientProfileEnglishLevel().ToList();
            //disabled = ClientProfileHasDisability().ToList();
            //license =
            //insurance =
            //age = ClientProfileWorkerAge().ToList();
            //race = ClientProfileRaceEthnicity().ToList();
            //incomeLevel = ClientProfileIncome().ToList()
            //gender = ClientProfileGender().ToList();

            ////TODO: rewrite all of this 
            //if (reportType == "weekly" || reportType == "monthly")
            //{
            //    getDates = Enumerable.Range(0, 1 + endDate.Subtract(beginDate).Days)
            //       .Select(offset => endDate.AddDays(-offset))
            //       .ToArray();

            //    q = getDates
            //        .Select(x => new WorkerData
            //        {
            //            dateStart = x,
            //            dateEnd = x.AddDays(1),
            //            singleAdults = singleAdults.Where(y => y.expDate >= x && y.memDate < x.AddDays(1)).Count(),
            //            familyHouseholds = familyHouseholds.Where(y => y.expDate >= x && y.memDate < x.AddDays(1)).Count(),
            //            newSingleAdults = singleAdults.Where(y => y.memDate >= x && y.memDate < x.AddDays(1)).Count(),
            //            newFamilyHouseholds = familyHouseholds.Where(y => y.memDate >= x && y.memDate < x.AddDays(1)).Count(),
            //            zipCompleteness = singleAdults.Where(y => y.zip != null && y.expDate >= x && y.memDate < x.AddDays(1)).Count()
            //                            + familyHouseholds.Where(y => y.zip != null && y.expDate >= x && y.memDate < x.AddDays(1)).Count()
            //        });
            //}
            //else if (reportType == "yearly")
            //{
            //    getDates = Enumerable.Range(1, 4)
            //        .Select(offset => endDate.AddMonths(-offset * 3))
            //        .ToArray();

                q = dateRange
                    .Select(x => new WorkerData
                    {
                        date = x,
                        count = status.Where(w => w.date == x).Select()
                        
                    });
            //}
            //else throw new Exception("Report type must be \"weekly\", \"monthly\" or \"yearly\".");

            return q;
        }




        /// <summary>
        /// Jobs and Zip Codes controller. The jobs and zip codes report was
        /// initially requested by Mountain View and centers can see what their 
        /// orders are and where they're coming from.
        /// </summary>
        /// <param name="beginDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IEnumerable<EmployerModel> EmployerReportController(DateTime beginDate, DateTime endDate)
        {
            IEnumerable<ReportUnit> topZips;

            var dateRange = GetDateRange(beginDate, endDate);

            topZips = ListOrdersByZip().ToList();

            //return topZips;
        }

        #region helper methods
        public static IEnumerable<DateTime> GetDateRange(DateTime beginDate, DateTime endDate)
        {
            return Enumerable.Range(0, 1 + endDate.Subtract(beginDate).Days)
                    .Select(offset => beginDate.AddDays(offset));
        }
        #endregion 

    }
    #endregion

    #region Report Models
    // The standalone models in this section are mostly at the bottom. Models that serve
    // as units of larger reports are at the top. Most of the models extend from a class
    // called "ReportUnit" and the class and its derivatives are in the middle of this
    // section.

    public class AverageWageModel
    {
        public DateTime date { get; set; }
        public int hours { get; set; }
        public double wages { get; set; }
        public double avg { get; set; }
    }

    public class MemberDateModel
    {
        public int dwcnum { get; set; }
        public string zip { get; set; }
        public DateTime memDate { get; set; }
        public DateTime expDate { get; set; }
    }

    public class TypeOfDispatchModel
    {
        public DateTime date { get; set; }
        public int dwcount { get; set; }
        public int dwcountr { get; set; }
        public int hhcount { get; set; }
        public int hhcountr { get; set; }
    }

    public class StatusUnit : ReportUnit
    {
        public int? expiredOnDate { get; set; }
        public int? enrolledOnDate { get; set; }
    }

    /// <summary>
    /// This gets a little confusing because of all the
    /// different "Zip" models. ZipUnit is a direct
    /// extension of ReportUnit and includes a space
    /// for a zip code string.
    /// </summary>
    public class ZipUnit : ReportUnit
    {
        public string zip { get; set; }
    }

    public class RaceUnit : ReportUnit
    {
        public int afroamerican { get; set; }
        public int asian { get; set; }
        public int caucasian { get; set; }
        public int hawaiian { get; set; }
        public int latino { get; set; }
        public int nativeamerican { get; set; }
        public int other { get; set; }
    }

    /// <summary>
    /// This is the basic unit of the Report service layer.
    /// All of the values are nullable to make the unit as
    /// extensible as possible.
    /// </summary>
    public class ReportUnit
    {
        public DateTime? date { get; set; }
        public int? count { get; set; }
        public string info { get; set; }
    }

    public class DailySumData : TypeOfDispatchModel
    {
        public int totalSignins { get; set; }
        public int uniqueSignins { get; set; }
        public int cancelledJobs { get; set; }
        public int totalAssignments { get; set; }
    }
    /// <summary>
    /// A class to contain the data for the Weekly Report
    /// int totalSignins, int noWeekJobs, int weekJobsSector, int
    /// weekEstDailyHours, double weekEstPayment, double weekHourlyWage
    /// </summary>
    public class WeeklySumData
    {
        public DayOfWeek dayofweek { get; set; }
        public DateTime date { get; set; }
        public int totalSignins { get; set; }
        public int numWeekJobs { get; set; }
        public int weekEstDailyHours { get; set; }
        public double weekEstPayment { get; set; }
        public double weekHourlyWage { get; set; }
        public IEnumerable<ReportUnit> topJobs { get; set; }
    }

    /// <summary>
    /// A class containing all of the data for the Monthly Report with Details
    /// DateTime date, int totalDWCSignins, int totalHHHSignins
    /// dispatchedDWCSignins, int dispatchedHHHSignins
    /// </summary>
    public class DispatchData
    {
        public DateTime dateStart { get; set; }
        public DateTime dateEnd { get; set; }
        public int totalSignins { get; set; }
        public int uniqueSignins { get; set; }
        public int dispatched { get; set; }
        public int tempDispatched { get; set; }
        public int permanentPlacements { get; set; }
        public int undupDispatched { get; set; }
        public int cancelledAssignments { get; set; }
        public int countNotAssigned { get; set; }
        public int totalHours { get; set; }
        public double totalIncome { get; set; }
        public double avgIncomePerHour { get; set; }
        public IEnumerable<ReportUnit> skills { get; set; }
    }

    public class EmployerModel
    {
        public string zips { get; set; }
        public int jobs { get; set; }
        public int emps { get; set; }
        public IEnumerable<ReportUnit> skills { get; set; }
    }

    public class WorkerData : ReportUnit
    {
        public DateTime? dateStart { get; set; }
        public DateTime? dateEnd { get; set; }
        public int active { get; set; }
        public int newlyEnrolled { get; set; }
        public int peopleWhoLeft { get; set; }
        public int singleAdults { get; set; }
        public int familyHouseholds { get; set; }
        public int newSingleAdults { get; set; }
        public int newFamilyHouseholds { get; set; }
        public int zipCompleteness { get; set; }
        
    }
    
    public class ActivityData
    {
        public string ActivityName { get; set; }
        public int Attendance { get; set; }
        public int MoreThanXHours { get; set; }
        
    }

    #endregion
}