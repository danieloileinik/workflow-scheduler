using Microsoft.EntityFrameworkCore;
using OrderDemo.Contracts;

namespace OrderDemo.Orders;

public sealed class OrderDb(DbContextOptions<OrderDb> options) : DbContext(options)
{
    public DbSet<DemoOrder> Orders => Set<DemoOrder>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DemoOrder>(order =>
        {
            order.ToTable("orders");
            order.HasKey(value => value.Id);
            order.Property(value => value.Id).ValueGeneratedNever();
            order.Property(value => value.Amount).HasPrecision(12, 2);
            order.Property(value => value.PaymentFailure).HasConversion<string>().HasMaxLength(16);
        });
    }
}
