/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#pragma once
#include "request_dealer/request_dealer.h"

namespace kunlun {
class BackUpDealer : public RequestDealer {
  typedef RequestDealer super;

public:
  explicit BackUpDealer(const char *request_json_str)
      : super(request_json_str) {}

  virtual ~BackUpDealer() {}
  bool virtual Deal() override;
  bool virtual constructCommand() override;
  void virtual AppendExtraToResponse(Json::Value &) override;

private:
};
} // namespace kunlun
