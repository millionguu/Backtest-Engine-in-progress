class Rebalance:
    def __init__(
        self, period, portfolio, factor, blacklist, disable_rebalance=False
    ) -> None:
        self.period = period
        self.portfolio = portfolio
        self.factor = factor
        self.blacklist = blacklist
        self.disable_rebalance = disable_rebalance

    def run(self, iter_index):
        if self.disable_rebalance:
            return
        cur_date = self.portfolio.date_df.item(iter_index, 0)
        position = self.factor.get_position(cur_date)

        residual = 0
        valid_count = 0
        new_position = []
        for s, w in position:
            if s not in self.blacklist:
                valid_count += 1
                if valid_count < len(position):
                    new_position.append((s, w))
                else:
                    new_position.append((s, w - 0.01))  # rounding error
            else:
                new_position.append((s, 0))
                residual += w

        if residual > 0:
            residual -= 0.01  # rounding error
            new_position = [
                (s, w + round(residual / valid_count, 3)) if w != 0 else (s, 0)
                for s, w in new_position
            ]

        position_change = []

        new_securities = [s for s, _ in new_position]
        for security in self.portfolio.security_book.keys():
            original_weight = self.portfolio.get_security_weight(security, iter_index)
            if security not in new_securities and original_weight > 0:
                position_change.append((security, -original_weight))

        for security, weight in new_position:
            original_weight = self.portfolio.get_security_weight(security, iter_index)
            position_change.append((security, weight - original_weight))

        # sold first and then buy
        position_change.sort(key=lambda p: p[1])
        print(
            f"rebalance on {cur_date}: {list(map(lambda t: (t[0].display(), round(t[1],3)), position_change))}"
        )

        turnover = sum((map(lambda t: abs(t[1]), position_change)))
        self.portfolio.value_book[iter_index]["turnover"] = turnover
        sector = ",".join((map(lambda t: t[0].sector, position_change)))
        self.portfolio.value_book[iter_index]["sector"] = sector

        for security, weight_change in position_change:
            if weight_change < 0:
                self.portfolio.reduce_security_weight(
                    security, abs(weight_change), iter_index
                )
            if weight_change > 0:
                self.portfolio.add_security_weight(security, weight_change, iter_index)
