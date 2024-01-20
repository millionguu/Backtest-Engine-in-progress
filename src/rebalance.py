class Rebalance:
    def __init__(
        self, period, portfolio, factor, blacklist, disable_rebalance=False
    ) -> None:
        self.period = period
        self.portfolio = portfolio
        self.factor = factor
        self.blacklist = blacklist
        self.disable_rebalance = disable_rebalance

    def run(self, cur_date):
        if self.disable_rebalance:
            return
        position = self.factor.get_position(cur_date)
        optimal_split = 1 / max(1, len(position) - len(self.blacklist))
        suboptimal_split = round(optimal_split, 2)
        weight = (
            suboptimal_split
            if suboptimal_split < optimal_split
            else suboptimal_split - 0.01
        )
        position = [
            (s, weight) if s not in self.blacklist else (s, 0) for s, w in position
        ]
        position_change = []

        new_securities = [s for s, _ in position]
        for security in self.portfolio.security_book.keys():
            condition = self.portfolio.security_book[security]["date"] == cur_date
            original_weight = self.portfolio.security_book[security][condition][
                "weight"
            ].iloc[0]
            if security not in new_securities and original_weight > 0:
                # TODO: rounding error
                weight = (original_weight // 0.001) * 0.001
                position_change.append((security, -weight))

        for security, weight in position:
            condition = self.portfolio.security_book[security]["date"] == cur_date
            original_weight = self.portfolio.security_book[security][condition][
                "weight"
            ].iloc[0]
            position_change.append((security, weight - original_weight))

        # sold first and then buy
        position_change.sort(key=lambda p: p[1])
        print(f"rebalance on {cur_date}: {position_change}")

        for security, weight_change in position_change:
            if weight_change < 0:
                self.portfolio.reduce_security_weight(
                    security, abs(weight_change), cur_date
                )
            elif weight_change > 0:
                self.portfolio.add_security_weight(security, weight_change, cur_date)
            else:
                pass
